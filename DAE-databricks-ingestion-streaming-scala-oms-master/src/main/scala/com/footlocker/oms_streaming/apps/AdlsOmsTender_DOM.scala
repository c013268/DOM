package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp_Gen2
import com.footlocker.oms_streaming.apps.AdlsLoadMain_Gen2.sparkSession
import com.footlocker.services.ADLSService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, prepareDf, readSqlFile}
import com.footlocker.utils.Common_Gen2.{processSubstitutions, stringToMap}
import com.footlocker.utils.Logging
import com.footlocker.utils.AuditUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.sql.Timestamp
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


object AdlsOmsTender_DOM extends Logging{


  private def extractViewName(stmt: String): String = {
    val upper = stmt.toUpperCase.trim
    val marker = "CREATE OR REPLACE TEMP VIEW"
    if (upper.contains(marker)) upper.split(marker)(1).trim.split("\\s+")(0).trim
    else "UNKNOWN"
  }

  def dom_tenders_load(spark: SparkSession, oms_parsed_tender_table: String, loadType: String, varSubstitutions: String) : Unit = {

    val jobName = "DOM_OMS_TENDERS_LOAD"
    val targetTable = "OMS_TENDERS"
    val auditTable = "prod.etl_stats_npii.dom_oms_etl_audit"
    val runId = s"${jobName}_${System.currentTimeMillis()}"

    val upperLoadType = Option(loadType).map(_.trim.toUpperCase).getOrElse("INCREMENTAL")

    val lastProcessedTs = getEffectiveWatermark(spark, auditTable, jobName, upperLoadType)
    println(s"Last processed watermark: $lastProcessedTs")

    // ─── Watermark-driven lookback dates ───────────────────────────────
    val baseMap = stringToMap(varSubstitutions)
    val sourceLookbackDays = baseMap.get("source_lookback_days")
      .flatMap(v => scala.util.Try(v.trim.toInt).toOption)
      .getOrElse(10000)
    val mergeTargetBufferDays = 100

    val lookbackDate = lastProcessedTs.toLocalDateTime.toLocalDate
      .minusDays(sourceLookbackDays)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val mergeTargetDate = lastProcessedTs.toLocalDateTime.toLocalDate
      .minusDays(sourceLookbackDays + mergeTargetBufferDays)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val mergeTargetPredicate = s"and t.load_date >= '$mergeTargetDate'"
    println(s"  [MERGE] mergeTargetDate=$mergeTargetDate (lookbackDate=$lookbackDate - ${mergeTargetBufferDays}d buffer)")
    // ─── End Watermark-driven lookback dates ───────────────────────────

    insertAuditStart(spark, auditTable, jobName, targetTable, runId, upperLoadType)

    var tendersDf: Option[DataFrame] = None

    try {

      import spark.implicits._

      println("Tenders load processing")

      val sqlFileName = "sqls/DOM-OMS_TENDERS.sql"
      val statements = {
        logInfo(s"Reading tenders SQL from resource file: $sqlFileName")
        val rawSql = readSqlFile(sqlFileName)

        println(s"  [SUB] lookback_date=$lookbackDate (source_lookback_days=$sourceLookbackDays, lastProcessedTs=$lastProcessedTs)")
        val substitutionMap = baseMap ++ Map(
            "incremental_ts" -> lastProcessedTs.toString,
            "lookback_date"  -> lookbackDate
          )
        val sourceSql = processSubstitutions(rawSql, substitutionMap)
        val stmts = sourceSql.split(";").map(_.trim).filter(_.nonEmpty)
        stmts.zipWithIndex.foreach { case (s, i) => println(s"  Parsed SQL[$i]: ${extractViewName(s)}") }
        stmts
      }

      // -------------------------------------------------------
      // SQL EXECUTION
      // -------------------------------------------------------
      statements.zipWithIndex.foreach { case (stmt, i) =>
        val viewName = extractViewName(stmt)
        val t = System.currentTimeMillis()
        spark.sql(stmt)
        println(s"    [SEQ] $viewName → ${System.currentTimeMillis() - t}ms")
      }

      // Cache & materialize tenders DF eagerly
      val tenders = {
        val df = spark.sql(s"select * from tenders WHERE load_date > '$lastProcessedTs'")
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        val cnt = df.count()
        println(s"  tenders cached: $cnt rows")
        df
      }
      tendersDf = Some(tenders)

      // Pre-compute watermark while DF is cached
      val newMaxTs: Timestamp =
        Option(tenders.agg(max("load_date").cast("timestamp")).as[Timestamp].first()).getOrElse(lastProcessedTs)

      //writing into tenders via MERGE
      tenders.createOrReplaceTempView("tenders_landing_merge")
      spark.sql(
        s"""
            MERGE INTO $oms_parsed_tender_table t
            USING tenders_landing_merge s
            ON s.orderId = t.orderId
            AND s.paymentTransactions_transaction_id = t.paymentTransactions_transaction_id
            AND t.ref_source = 'MAO'
            $mergeTargetPredicate
            WHEN MATCHED THEN UPDATE SET
              t.currencyIso = s.currencyIso,
              t.messageType = s.messageType,
              t.omsOrderId = s.omsOrderId,
              t.companyNumber = s.companyNumber,
              t.orderStatus = s.orderStatus,
              t.orderHeader_discountedTotalAmount = s.orderHeader_discountedTotalAmount,
              t.load_date = s.load_date,
              t.authorizations_paymentType = s.authorizations_paymentType,
              t.authorizations_authAmount = s.authorizations_authAmount,
              t.authorizations_authCode = s.authorizations_authCode,
              t.authorizations_gateway = s.authorizations_gateway,
              t.authorizations_paymentVendor = s.authorizations_paymentVendor,
              t.authorizations_status = s.authorizations_status,
              t.authorizations_transactionDate = s.authorizations_transactionDate,
              t.authorizations_transactionId = s.authorizations_transactionId,
              t.paymentTransactions_consignmentId = s.paymentTransactions_consignmentId,
              t.paymentTransactions_refundId = s.paymentTransactions_refundId,
              t.attributes_authResponse = s.attributes_authResponse,
              t.attributes_avsCode = s.attributes_avsCode,
              t.attributes_cardAlias = s.attributes_cardAlias,
              t.attributes_cardBin = s.attributes_cardBin,
              t.attributes_cardLast4 = s.attributes_cardLast4,
              t.attributes_cardToken = s.attributes_cardToken,
              t.attributes_cardType = s.attributes_cardType,
              t.attributes_cardTypeDisplay = s.attributes_cardTypeDisplay,
              t.attributes_confirmationCode = s.attributes_confirmationCode,
              t.attributes_cvvResponse = s.attributes_cvvResponse,
              t.attributes_expirationDate = s.attributes_expirationDate,
              t.attributes_fundingSource = s.attributes_fundingSource,
              t.attributes_sellerProtectionStatus = s.attributes_sellerProtectionStatus,
              t.paymentTransactions_transaction_amount = s.paymentTransactions_transaction_amount,
              t.paymentTransactions_transaction_authCode = s.paymentTransactions_transaction_authCode,
              t.paymentTransactions_transaction_cardLast4 = s.paymentTransactions_transaction_cardLast4,
              t.paymentTransactions_transaction_creditCardType = s.paymentTransactions_transaction_creditCardType,
              t.paymentTransactions_transaction_date = s.paymentTransactions_transaction_date,
              t.paymentTransactions_transaction_id = s.paymentTransactions_transaction_id,
              t.paymentTransactions_transaction_paymentTransactionRequestId = s.paymentTransactions_transaction_paymentTransactionRequestId,
              t.paymentTransactions_transaction_paymentTransactionType = s.paymentTransactions_transaction_paymentTransactionType,
              t.paymentTransactions_transaction_paymentType = s.paymentTransactions_transaction_paymentType,
              t.paymentTransactions_transaction_status = s.paymentTransactions_transaction_status,
              t.authorizations_preSettled = s.authorizations_preSettled,
              t.authorizations_originalOrderNumber = s.authorizations_originalOrderNumber,
              t.attributes_storeMerchantId = s.attributes_storeMerchantId,
              t.attributes_storeTerminalId = s.attributes_storeTerminalId,
              t.attributes_storeOrderRequestId = s.attributes_storeOrderRequestId,
              t.attributes_storeInvoiceId = s.attributes_storeInvoiceId,
              t.attributes_giftCardNumber = s.attributes_giftCardNumber,
              t.attributes_email = s.attributes_email,
              t.attributes_payerStatus = s.attributes_payerStatus,
              t.ref_source = s.ref_source
            WHEN NOT MATCHED THEN INSERT (currencyIso, messageType, orderId, omsOrderId, companyNumber, orderStatus, orderHeader_discountedTotalAmount, load_date, authorizations_paymentType, authorizations_authAmount, authorizations_authCode, authorizations_gateway, authorizations_paymentVendor, authorizations_status, authorizations_transactionDate, authorizations_transactionId, paymentTransactions_consignmentId, paymentTransactions_refundId, attributes_authResponse, attributes_avsCode, attributes_cardAlias, attributes_cardBin, attributes_cardLast4, attributes_cardToken, attributes_cardType, attributes_cardTypeDisplay, attributes_confirmationCode, attributes_cvvResponse, attributes_expirationDate, attributes_fundingSource, attributes_sellerProtectionStatus, paymentTransactions_transaction_amount, paymentTransactions_transaction_authCode, paymentTransactions_transaction_cardLast4, paymentTransactions_transaction_creditCardType, paymentTransactions_transaction_date, paymentTransactions_transaction_id, paymentTransactions_transaction_paymentTransactionRequestId, paymentTransactions_transaction_paymentTransactionType, paymentTransactions_transaction_paymentType, paymentTransactions_transaction_status, authorizations_preSettled, authorizations_originalOrderNumber, attributes_storeMerchantId, attributes_storeTerminalId, attributes_storeOrderRequestId, attributes_storeInvoiceId, attributes_giftCardNumber, attributes_email, attributes_payerStatus, ref_source)
            VALUES (s.currencyIso, s.messageType, s.orderId, s.omsOrderId, s.companyNumber, s.orderStatus, s.orderHeader_discountedTotalAmount, s.load_date, s.authorizations_paymentType, s.authorizations_authAmount, s.authorizations_authCode, s.authorizations_gateway, s.authorizations_paymentVendor, s.authorizations_status, s.authorizations_transactionDate, s.authorizations_transactionId, s.paymentTransactions_consignmentId, s.paymentTransactions_refundId, s.attributes_authResponse, s.attributes_avsCode, s.attributes_cardAlias, s.attributes_cardBin, s.attributes_cardLast4, s.attributes_cardToken, s.attributes_cardType, s.attributes_cardTypeDisplay, s.attributes_confirmationCode, s.attributes_cvvResponse, s.attributes_expirationDate, s.attributes_fundingSource, s.attributes_sellerProtectionStatus, s.paymentTransactions_transaction_amount, s.paymentTransactions_transaction_authCode, s.paymentTransactions_transaction_cardLast4, s.paymentTransactions_transaction_creditCardType, s.paymentTransactions_transaction_date, s.paymentTransactions_transaction_id, s.paymentTransactions_transaction_paymentTransactionRequestId, s.paymentTransactions_transaction_paymentTransactionType, s.paymentTransactions_transaction_paymentType, s.paymentTransactions_transaction_status, s.authorizations_preSettled, s.authorizations_originalOrderNumber, s.attributes_storeMerchantId, s.attributes_storeTerminalId, s.attributes_storeOrderRequestId, s.attributes_storeInvoiceId, s.attributes_giftCardNumber, s.attributes_email, s.attributes_payerStatus, s.ref_source)
         """)

      updateAuditSuccess(spark, auditTable, runId, newMaxTs)

      println("Tenders load competed")
    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        updateAuditFailure(spark, auditTable, runId, s"Source table not found: ${ex.getMessage}")
        logError(s"Tenders load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex

      case ex: Exception =>
        updateAuditFailure(spark, auditTable, runId, ex.getMessage)
        logError(s"Tenders load failed with a general exception: ${ex.getMessage}", ex)
        throw ex

    } finally {
      tendersDf.foreach(_.unpersist())
    }
  }
}
