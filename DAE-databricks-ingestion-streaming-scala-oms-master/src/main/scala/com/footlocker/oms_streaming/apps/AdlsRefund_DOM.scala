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


object AdlsRefund_DOM extends Logging{


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

  def dom_refund_load(spark: SparkSession, refund_refined_table: String, refund_landing_table: String, loadType: String, varSubstitutions: String): Unit =  {

    val jobName = "DOM_OMS_REFUNDS_LOAD"
    val targetTable = "OMS_REFUNDS"
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

    val mergeTargetPredicate = s"and t.order_date >= '$mergeTargetDate'"
    println(s"  [MERGE] mergeTargetDate=$mergeTargetDate (lookbackDate=$lookbackDate - ${mergeTargetBufferDays}d buffer)")
    // ─── End Watermark-driven lookback dates ───────────────────────────

    insertAuditStart(spark, auditTable, jobName, targetTable, runId, upperLoadType)

    var refundsDf: Option[DataFrame] = None
    var executorService: Option[ExecutorService] = None

    try {

      import spark.implicits._

      println("Refunds load processing")

      val sqlFileName = "sqls/DOM-OMS_REFUNDS.sql"
      val statements = {
        logInfo(s"Reading SQL from resource file: $sqlFileName")
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
      // PARALLEL SQL EXECUTION (tiered by view dependencies)
      // -------------------------------------------------------
      val sqlExecutor = Executors.newFixedThreadPool(3)
      val sqlEc: ExecutionContext = ExecutionContext.fromExecutorService(sqlExecutor)
      try {
        val tier0End = Math.min(3, statements.length)
        val tier1End = Math.min(4, statements.length)
        registerViewsParallel(spark, statements.slice(0, tier0End), "T0", sqlEc)

        if (statements.length > tier0End) {
          registerViewsParallel(spark, statements.slice(tier0End, tier1End), "T1", sqlEc)
        }

        if (statements.length > tier1End) {
          registerViewsParallel(spark, statements.slice(tier1End, statements.length), "T2", sqlEc)
        }
      } finally {
        sqlExecutor.shutdown()
      }

      // Cache & materialize refunds DF eagerly
      val refunds = {
        val df = spark.sql(s"select * from refunds")
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        val cnt = df.count()
        println(s"  refunds cached: $cnt rows")
        df
      }
      refundsDf = Some(refunds)
      val refunds_ref = refunds
      refunds.createOrReplaceTempView("refunds")

      // Pre-compute watermark while DF is cached
      val newMaxTs: Timestamp =
        Option(refunds.agg(max("load_time_adls").cast("timestamp")).as[Timestamp].first()).getOrElse(lastProcessedTs)

      // Prepare refined DF and register temp view
      val refunds_refined = {
        refunds_ref.filter("productNumber is not null").withColumn(
          "returnDate",
          date_format(
            to_timestamp(col("returnDate"), "yyyy-MM-dd HH:mm:ss.SSS"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
          ).cast("string")
        )
        .withColumn(
          "postedAt",
          date_format(
            to_timestamp(col("postedAt"), "yyyy-MM-dd HH:mm:ss.SSS"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
          ).cast("string")
        )
      }
      refunds_refined.createOrReplaceTempView("refunds_refined")

      executorService = Some(Executors.newFixedThreadPool(2))
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService.get)

      // Landing + Delta merge in parallel
      val landingFuture = Future {
        val refunds_final = refunds.drop("updated_datetime").withColumn(
            "returnDate",
            date_format(
              to_timestamp(col("returnDate"), "yyyy-MM-dd HH:mm:ss.SSS"),
              "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
            ).cast("string")
          )
          .withColumn(
            "postedAt",
            date_format(
              to_timestamp(col("postedAt"), "yyyy-MM-dd HH:mm:ss.SSS"),
              "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
            ).cast("string")
          ).filter("productNumber is not null")

      refunds_final.createOrReplaceTempView("refunds_landing_merge")
	  spark.sql(
        s"""
			MERGE INTO $refund_landing_table t
			USING refunds_landing_merge s
			ON s.order_id = t.order_id
				AND s.refundId = t.refundId
				AND s.companynumber = t.companynumber
				AND s.lineNumber = t.lineNumber
				AND s.refund_status = t.refund_status
				AND t.ref_source = 'MAO'
				$mergeTargetPredicate
			WHEN MATCHED THEN UPDATE SET
				t.order_datetime = s.order_datetime,
				t.refund_header_date = s.refund_header_date,
				t.refund_status = s.refund_status,
				t.refund_createdBy = s.refund_createdBy,
				t.refund_userId = s.refund_userId,
				t.source = s.source,
				t.refundNum = s.refundNum,
				t.quantity = s.quantity,
				t.name = s.name,
				t.sku = s.sku,
				t.size = s.size,
				t.color = s.color,
				t.image = s.image,
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
				t.cegrRefId = s.cegrRefId,
				t.originalRetailPrice = s.originalRetailPrice,
				t.originalUnitPrice = s.originalUnitPrice,
				t.originalUnitDiscountAmount = s.originalUnitDiscountAmount,
				t.lineRefundSubTotal = s.lineRefundSubTotal,
				t.taxAmount = s.taxAmount,
				t.shippingAmount = s.shippingAmount,
				t.shippingTaxAmount = s.shippingTaxAmount,
				t.inboundShippingAmount = s.inboundShippingAmount,
				t.totalAmount = s.totalAmount,
				t.returnExpected = s.returnExpected,
				t.returned = s.returned,
				t.returnNumbers = s.returnNumbers,
				t.quantityReturned = s.quantityReturned,
				t.returnDate = s.returnDate,
				t.reasonCode = s.reasonCode,
				t.ih_inboundShippingAmount = s.ih_inboundShippingAmount,
				t.ih_lineRefundSubTotal = s.ih_lineRefundSubTotal,
				t.ih_originalRetailPrice = s.ih_originalRetailPrice,
				t.ih_originalUnitDiscountAmount = s.ih_originalUnitDiscountAmount,
				t.ih_originalUnitPrice = s.ih_originalUnitPrice,
				t.ih_shippingAmount = s.ih_shippingAmount,
				t.ih_shippingTaxAmount = s.ih_shippingTaxAmount,
				t.ih_taxAmount = s.ih_taxAmount,
				t.ih_totalAmount = s.ih_totalAmount,
				t.refundMethods = s.refundMethods,
				t.paymentsInfo = s.paymentsInfo,
				t.refund_notes = s.refund_notes,
				t.refunds_statushistory = s.refunds_statushistory,
				t.load_time_kafka = s.load_time_kafka,
				t.load_time_adls = s.load_time_adls,
				t.order_date = s.order_date,
				t.postedAt = s.postedAt,
				t.postedBy = s.postedBy,
				t.ref_source = s.ref_source
			WHEN NOT MATCHED THEN INSERT (order_id, order_datetime, refundId, companyNumber, refund_header_date, refund_status, refund_createdBy, refund_userId, source, refundNum, quantity, name, sku, size, color, image, brand, category, description, isCollectUpFront, backorderFlag, launchSkuFlag, taxCode, productDesignator, productNumber, productType, cegrRefId, originalRetailPrice, originalUnitPrice, originalUnitDiscountAmount, lineRefundSubTotal, taxAmount, shippingAmount, shippingTaxAmount, inboundShippingAmount, totalAmount, returnExpected, returned, returnNumbers, quantityReturned, returnDate, reasonCode, ih_inboundShippingAmount, ih_lineRefundSubTotal, ih_originalRetailPrice, ih_originalUnitDiscountAmount, ih_originalUnitPrice, ih_shippingAmount, ih_shippingTaxAmount, ih_taxAmount, ih_totalAmount, refundMethods, paymentsInfo, refund_notes, refunds_statusHistory, load_time_kafka, load_time_adls, order_date, postedAt, postedBy, lineNumber, ref_source)
			VALUES (s.order_id, s.order_datetime, s.refundId, s.companynumber, s.refund_header_date, s.refund_status, s.refund_createdBy, s.refund_userId, s.source, s.refundNum, s.quantity, s.name, s.sku, s.size, s.color, s.image, s.brand, s.category, s.description, s.isCollectUpFront, s.backorderFlag, s.launchSkuFlag, s.taxCode, s.productDesignator, s.productNumber, s.productType, s.cegrRefId, s.originalRetailPrice, s.originalUnitPrice, s.originalUnitDiscountAmount, s.lineRefundSubTotal, s.taxAmount, s.shippingAmount, s.shippingTaxAmount, s.inboundShippingAmount, s.totalAmount, s.returnExpected, s.returned, s.returnNumbers, s.quantityReturned, s.returnDate, s.reasonCode, s.ih_inboundShippingAmount, s.ih_lineRefundSubTotal, s.ih_originalRetailPrice, s.ih_originalUnitDiscountAmount, s.ih_originalUnitPrice, s.ih_shippingAmount, s.ih_shippingTaxAmount, s.ih_taxAmount, s.ih_totalAmount, s.refundMethods, s.paymentsInfo, s.refund_notes, s.refunds_statushistory, s.load_time_kafka, s.load_time_adls, s.order_date, s.postedAt, s.postedBy, s.lineNumber, s.ref_source)
       """)
      }

      val refinedFuture = Future {
        spark.sql(
        s"""
            MERGE INTO $refund_refined_table t
            USING refunds_refined s
            ON s.order_id = t.order_id
				AND s.refundId = t.refundId
				AND s.companynumber = t.companynumber
				AND s.lineNumber = t.lineNumber
				AND t.ref_source = 'MAO'
				$mergeTargetPredicate
            WHEN MATCHED THEN UPDATE SET
				t.refund_status = s.refund_status,
				t.order_datetime = s.order_datetime,
				t.refund_header_date = s.refund_header_date,
				t.refund_createdBy = s.refund_createdBy,
				t.refund_userId = s.refund_userId,
				t.source = s.source,
				t.refundNum = s.refundNum,
				t.quantity = s.quantity,
				t.name = s.name,
				t.sku = s.sku,
				t.size = s.size,
				t.color = s.color,
				t.image = s.image,
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
				t.cegrRefId = s.cegrRefId,
				t.originalRetailPrice = s.originalRetailPrice,
				t.originalUnitPrice = s.originalUnitPrice,
				t.originalUnitDiscountAmount = s.originalUnitDiscountAmount,
				t.lineRefundSubTotal = s.lineRefundSubTotal,
				t.taxAmount = s.taxAmount,
				t.shippingAmount = s.shippingAmount,
				t.shippingTaxAmount = s.shippingTaxAmount,
				t.inboundShippingAmount = s.inboundShippingAmount,
				t.totalAmount = s.totalAmount,
				t.returnExpected = s.returnExpected,
				t.returned = s.returned,
				t.returnNumbers = s.returnNumbers,
				t.quantityReturned = s.quantityReturned,
				t.returnDate = s.returnDate,
				t.reasonCode = s.reasonCode,
				t.ih_inboundShippingAmount = s.ih_inboundShippingAmount,
				t.ih_lineRefundSubTotal = s.ih_lineRefundSubTotal,
				t.ih_originalRetailPrice = s.ih_originalRetailPrice,
				t.ih_originalUnitDiscountAmount = s.ih_originalUnitDiscountAmount,
				t.ih_originalUnitPrice = s.ih_originalUnitPrice,
				t.ih_shippingAmount = s.ih_shippingAmount,
				t.ih_shippingTaxAmount = s.ih_shippingTaxAmount,
				t.ih_taxAmount = s.ih_taxAmount,
				t.ih_totalAmount = s.ih_totalAmount,
				t.refundMethods = s.refundMethods,
				t.paymentsInfo = s.paymentsInfo,
				t.refund_notes = s.refund_notes,
				t.refunds_statushistory = s.refunds_statushistory,
				t.load_time_kafka = s.load_time_kafka,
				t.load_time_adls = s.load_time_adls,
				t.order_date = s.order_date,
				t.postedAt = s.postedAt,
				t.postedBy = s.postedBy,
				t.ref_source = s.ref_source
			WHEN NOT MATCHED THEN INSERT (order_id, order_datetime, refundId, companyNumber, refund_header_date, refund_status, refund_createdBy, refund_userId, source, refundNum, quantity, name, sku, size, color, image, brand, category, description, isCollectUpFront, backorderFlag, launchSkuFlag, taxCode, productDesignator, productNumber, productType, cegrRefId, originalRetailPrice, originalUnitPrice, originalUnitDiscountAmount, lineRefundSubTotal, taxAmount, shippingAmount, shippingTaxAmount, inboundShippingAmount, totalAmount, returnExpected, returned, returnNumbers, quantityReturned, returnDate, reasonCode, ih_inboundShippingAmount, ih_lineRefundSubTotal, ih_originalRetailPrice, ih_originalUnitDiscountAmount, ih_originalUnitPrice, ih_shippingAmount, ih_shippingTaxAmount, ih_taxAmount, ih_totalAmount, refundMethods, paymentsInfo, refund_notes, refunds_statusHistory, load_time_kafka, load_time_adls, order_date, postedAt, postedBy, lineNumber, ref_source)
			VALUES (s.order_id, s.order_datetime, s.refundId, s.companynumber, s.refund_header_date, s.refund_status, s.refund_createdBy, s.refund_userId, s.source, s.refundNum, s.quantity, s.name, s.sku, s.size, s.color, s.image, s.brand, s.category, s.description, s.isCollectUpFront, s.backorderFlag, s.launchSkuFlag, s.taxCode, s.productDesignator, s.productNumber, s.productType, s.cegrRefId, s.originalRetailPrice, s.originalUnitPrice, s.originalUnitDiscountAmount, s.lineRefundSubTotal, s.taxAmount, s.shippingAmount, s.shippingTaxAmount, s.inboundShippingAmount, s.totalAmount, s.returnExpected, s.returned, s.returnNumbers, s.quantityReturned, s.returnDate, s.reasonCode, s.ih_inboundShippingAmount, s.ih_lineRefundSubTotal, s.ih_originalRetailPrice, s.ih_originalUnitDiscountAmount, s.ih_originalUnitPrice, s.ih_shippingAmount, s.ih_shippingTaxAmount, s.ih_taxAmount, s.ih_totalAmount, s.refundMethods, s.paymentsInfo, s.refund_notes, s.refunds_statushistory, s.load_time_kafka, s.load_time_adls, s.order_date, s.postedAt, s.postedBy, s.lineNumber, s.ref_source)
        """)
      }

      // Wait for all parallel paths
      val landingResult = scala.util.Try(Await.result(landingFuture, 4.hours))
      val deltaResult   = scala.util.Try(Await.result(refinedFuture, 4.hours))

      (landingResult, deltaResult) match {
        case (scala.util.Failure(ex), _) => throw ex
        case (_, scala.util.Failure(ex)) => throw ex
        case _ => // all succeeded
      }

      updateAuditSuccess(spark, auditTable, runId, newMaxTs)

      println("refunds load competed")
    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        updateAuditFailure(spark, auditTable, runId, s"Source table not found: ${ex.getMessage}")
        logError(s"Refunds load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex

      case ex: Exception =>
        updateAuditFailure(spark, auditTable, runId, ex.getMessage)
        logError(s"Refunds load failed with a general exception: ${ex.getMessage}", ex)
        throw ex

    } finally {
      refundsDf.foreach(_.unpersist())
      executorService.foreach(_.shutdown())
    }
  }
}
