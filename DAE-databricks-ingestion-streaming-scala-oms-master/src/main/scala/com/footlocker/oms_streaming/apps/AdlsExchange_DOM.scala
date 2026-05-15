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


object AdlsExchange_DOM extends Logging{

  private def extractViewName(stmt: String): String = {
    val upper = stmt.toUpperCase.trim
    val marker = "CREATE OR REPLACE TEMP VIEW"
    if (upper.contains(marker)) upper.split(marker)(1).trim.split("\\s+")(0).trim
    else "UNKNOWN"
  }

  private def registerViewsParallel(spark: SparkSession, stmts: Seq[String], tierLabel: String, ec: ExecutionContext): Unit = {
    implicit val iec: ExecutionContext = ec
    val futures = stmts.map { stmt =>
      val viewName = extractViewName(stmt)
      Future {
        val t = System.currentTimeMillis()
        spark.sql(stmt)
        println(s"    [PAR-$tierLabel] $viewName → ${System.currentTimeMillis() - t}ms")
      }
    }
    Await.result(Future.sequence(futures), 5.minutes)
  }

  def dom_exchanges_load(spark: SparkSession, exchange_landing_table: String, loadType: String, varSubstitutions: String) : Unit = {

    val jobName = "DOM_OMS_EXCHANGES_LOAD"
    val targetTable = "OMS_EXCHANGES"
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

    val mergeTargetPredicate = s"and t.orderdate >= '$mergeTargetDate'"
    println(s"  [MERGE] mergeTargetDate=$mergeTargetDate (lookbackDate=$lookbackDate - ${mergeTargetBufferDays}d buffer)")
    // ─── End Watermark-driven lookback dates ───────────────────────────

    insertAuditStart(spark, auditTable, jobName, targetTable, runId, upperLoadType)

    var exchangesDf: Option[DataFrame] = None

    try {

      import spark.implicits._

      println("Exchanges load processing")

      val sqlFileName = "sqls/DOM-EXCHANGES.sql"
      logInfo(s"Reading exchanges SQL from resource file: $sqlFileName")
      val rawSql = readSqlFile(sqlFileName)
      println(s"  [SUB] lookback_date=$lookbackDate (source_lookback_days=$sourceLookbackDays, lastProcessedTs=$lastProcessedTs)")
      val substitutionMap = baseMap ++ Map(
        "incremental_ts" -> lastProcessedTs.toString,
        "lookback_date"  -> lookbackDate
      )
      val sourceSql = processSubstitutions(rawSql, substitutionMap)
      val statements = sourceSql.split(";").map(_.trim).filter(_.nonEmpty)
      statements.zipWithIndex.foreach { case (s, i) => println(s"  Parsed SQL[$i]: ${extractViewName(s)}") }

      // -------------------------------------------------------
      // PARALLEL SQL EXECUTION (tiered by view dependencies)
      // -------------------------------------------------------
      val sqlExecutor = Executors.newFixedThreadPool(3)
      val sqlEc: ExecutionContext = ExecutionContext.fromExecutorService(sqlExecutor)
      try {
        val tier0End = Math.min(3, statements.length)
        registerViewsParallel(spark, statements.slice(0, tier0End), "T0", sqlEc)

        if (statements.length > tier0End) {
          registerViewsParallel(spark, statements.slice(tier0End, statements.length), "T1", sqlEc)
        }
      } finally {
        sqlExecutor.shutdown()
      }

      // Cache & materialize exchanges DF eagerly
      val exchanges = {
        val df = spark.sql(s"select * from exchanges").filter("productnumber is not null")
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        val cnt = df.count()
        println(s"  exchanges cached: $cnt rows")
        df
      }
      exchangesDf = Some(exchanges)

      // Pre-compute watermark while DF is cached
      val newMaxTs: Timestamp = {
        Option(exchanges.agg(max("load_time_adls").cast("timestamp")).as[Timestamp].first()).getOrElse(lastProcessedTs)
      }

      // Write to landing table via MERGE
      exchanges.createOrReplaceTempView("exchanges_landing_merge")
	  spark.sql(
        s"""
          MERGE INTO $exchange_landing_table t
          USING exchanges_landing_merge s
          ON s.order_id = t.order_id
			AND s.exchangeId = t.exchangeId
			AND s.companyNumber = t.companyNumber
			AND s.lineNumber = t.lineNumber
			AND s.exchange_status = t.exchange_status
			AND t.ref_source = 'MAO'
			$mergeTargetPredicate
          WHEN MATCHED THEN UPDATE SET
            t.Header_exchangeStatus = s.Header_exchangeStatus,
            t.exchange_createdBy = s.exchange_createdBy,
            t.userId = s.userId,
            t.source = s.source,
            t.exchange_header_date = s.exchange_header_date,
            t.exchangeNum = s.exchangeNum,
            t.returnNum = s.returnNum,
            t.exchangeOrderNum = s.exchangeOrderNum,
            t.fullfillmentType = s.fullfillmentType,
            t.shipMethod = s.shipMethod,
            t.shipMethodDesc = s.shipMethodDesc,
            t.shippingAmount_currencyIso = s.shippingAmount_currencyIso,
            t.shippingAmount_value = s.shippingAmount_value,
            t.shippingLine = s.shippingLine,
            t.line_saleCode = s.line_saleCode,
            t.line_taxCode = s.line_taxCode,
            t.line_ = s.line_,
            t.line_giftReceipientEmail = s.line_giftReceipientEmail,
            t.line_giftFrom = s.line_giftFrom,
            t.line_giftTo = s.line_giftTo,
            t.line_giftCardNum = s.line_giftCardNum,
            t.line_shipMethod = s.line_shipMethod,
            t.line_freeShipping = s.line_freeShipping,
            t.line_quantity = s.line_quantity,
            t.line_inventoryLocation = s.line_inventoryLocation,
            t.name = s.name,
            t.image = s.image,
            t.sku = s.sku,
            t.size = s.size,
            t.color = s.color,
            t.brand = s.brand,
            t.category = s.category,
            t.description = s.description,
            t.isCollectUpFront = s.isCollectUpFront,
            t.backorderFlag = s.backorderFlag,
            t.launchSkuFlag = s.launchSkuFlag,
            t.taxCode = s.taxCode,
            t.productDesignator = s.productDesignator,
            t.productNumber = s.productNumber,
            t.productType = s.productType,
            t.currencyIso = s.currencyIso,
            t.priceOverrideReason = s.priceOverrideReason,
            t.originalRetailPrice = s.originalRetailPrice,
            t.UnitPrice = s.UnitPrice,
            t.subTotalAmount = s.subTotalAmount,
            t.taxAmount = s.taxAmount,
            t.shippingAmount = s.shippingAmount,
            t.shippingTaxAmount = s.shippingTaxAmount,
            t.giftBoxAmount = s.giftBoxAmount,
            t.giftBoxTaxAmount = s.giftBoxTaxAmount,
            t.totalAmount = s.totalAmount,
            t.discountAmount = s.discountAmount,
            t.discountedTotalAmount = s.discountedTotalAmount,
            t.paymentRequest = s.paymentRequest,
            t.paymentsInfo = s.paymentsInfo,
            t.alternateShipping_firstName = s.alternateShipping_firstName,
            t.alternateShipping_lastName = s.alternateShipping_lastName,
            t.alternateShipping_email = s.alternateShipping_email,
            t.alternateShipping_companyName = s.alternateShipping_companyName,
            t.alternateShipping_phoneNumber = s.alternateShipping_phoneNumber,
            t.alternateShipping_addressLine1 = s.alternateShipping_addressLine1,
            t.alternateShipping_addressLine2 = s.alternateShipping_addressLine2,
            t.alternateShipping_city = s.alternateShipping_city,
            t.alternateShipping_state = s.alternateShipping_state,
            t.alternateShipping_stateCode = s.alternateShipping_stateCode,
            t.alternateShipping_country = s.alternateShipping_country,
            t.alternateShipping_countryCode = s.alternateShipping_countryCode,
            t.alternateShipping_postalCode = s.alternateShipping_postalCode,
            t.orderDateTime = s.orderDateTime,
            t.postedAt = s.postedAt,
            t.postedBy = s.postedBy,
            t.load_time_kafka = s.load_time_kafka,
            t.load_time_adls = s.load_time_adls,
            t.orderdate = s.orderdate,
            t.ref_source = s.ref_source
          WHEN NOT MATCHED THEN INSERT (order_id, exchangeId, Header_exchangeStatus, companyNumber, exchange_createdBy, userId, source, exchange_header_date, exchangeNum, returnNum, exchangeOrderNum, exchange_status, fullfillmentType, shipMethod, shipMethodDesc, shippingAmount_currencyIso, shippingAmount_value, shippingLine, lineNumber, line_saleCode, line_taxCode, line_, line_giftReceipientEmail, line_giftFrom, line_giftTo, line_giftCardNum, line_shipMethod, line_freeShipping, line_quantity, line_inventoryLocation, name, image, sku, size, color, brand, category, description, isCollectUpFront, backorderFlag, launchSkuFlag, taxCode, productDesignator, productNumber, productType, currencyIso, priceOverrideReason, originalRetailPrice, UnitPrice, subTotalAmount, taxAmount, shippingAmount, shippingTaxAmount, giftBoxAmount, giftBoxTaxAmount, totalAmount, discountAmount, discountedTotalAmount, paymentRequest, paymentsInfo, alternateShipping_firstName, alternateShipping_lastName, alternateShipping_email, alternateShipping_companyName, alternateShipping_phoneNumber, alternateShipping_addressLine1, alternateShipping_addressLine2, alternateShipping_city, alternateShipping_state, alternateShipping_stateCode, alternateShipping_country, alternateShipping_countryCode, alternateShipping_postalCode, orderDateTime, postedAt, postedBy, load_time_kafka, load_time_adls, orderdate, ref_source)
          VALUES (s.order_id, s.exchangeId, s.Header_exchangeStatus, s.companyNumber, s.exchange_createdBy, s.userId, s.source, s.exchange_header_date, s.exchangeNum, s.returnNum, s.exchangeOrderNum, s.exchange_status, s.fullfillmentType, s.shipMethod, s.shipMethodDesc, s.shippingAmount_currencyIso, s.shippingAmount_value, s.shippingLine, s.lineNumber, s.line_saleCode, s.line_taxCode, s.line_, s.line_giftReceipientEmail, s.line_giftFrom, s.line_giftTo, s.line_giftCardNum, s.line_shipMethod, s.line_freeShipping, s.line_quantity, s.line_inventoryLocation, s.name, s.image, s.sku, s.size, s.color, s.brand, s.category, s.description, s.isCollectUpFront, s.backorderFlag, s.launchSkuFlag, s.taxCode, s.productDesignator, s.productNumber, s.productType, s.currencyIso, s.priceOverrideReason, s.originalRetailPrice, s.UnitPrice, s.subTotalAmount, s.taxAmount, s.shippingAmount, s.shippingTaxAmount, s.giftBoxAmount, s.giftBoxTaxAmount, s.totalAmount, s.discountAmount, s.discountedTotalAmount, s.paymentRequest, s.paymentsInfo, s.alternateShipping_firstName, s.alternateShipping_lastName, s.alternateShipping_email, s.alternateShipping_companyName, s.alternateShipping_phoneNumber, s.alternateShipping_addressLine1, s.alternateShipping_addressLine2, s.alternateShipping_city, s.alternateShipping_state, s.alternateShipping_stateCode, s.alternateShipping_country, s.alternateShipping_countryCode, s.alternateShipping_postalCode, s.orderDateTime, s.postedAt, s.postedBy, s.load_time_kafka, s.load_time_adls, s.orderdate, s.ref_source)
       """)

      updateAuditSuccess(spark, auditTable, runId, newMaxTs)

      println("Exchanges load competed")
    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        updateAuditFailure(spark, auditTable, runId, s"Source table not found: ${ex.getMessage}")
        logError(s"Exchanges load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex

      case ex: Exception =>
        updateAuditFailure(spark, auditTable, runId, ex.getMessage)
        logError(s"Exchanges load failed with a general exception: ${ex.getMessage}", ex)
        throw ex

    } finally {
      exchangesDf.foreach(_.unpersist())
    }
  }
}
