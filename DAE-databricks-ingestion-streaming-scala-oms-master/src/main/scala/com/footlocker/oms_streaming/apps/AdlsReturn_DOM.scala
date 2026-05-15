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
import org.apache.spark.sql.types.{ArrayType, StringType, StructType, StructField}
import org.apache.spark.sql.functions.{col, from_json}
import java.sql.Timestamp
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


object AdlsReturn_DOM extends Logging{


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

  def dom_return_load(spark: SparkSession, return_refined_table: String, return_landing_table: String, loadType: String, varSubstitutions: String): Unit = {

    val jobName = "DOM_OMS_RETURNS_LOAD"
    val targetTable = "OMS_RETURNS"
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

    var returnsRefinedDf: Option[DataFrame] = None
    var executorService: Option[ExecutorService] = None

    try {

      import spark.implicits._

      println("Return load processing")

      val sqlFileName = "sqls/DOM-OMS_RETURNS.sql"
      val statements = {
        logInfo(s"Reading RETURNS SQL from resource file: $sqlFileName")
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
        registerViewsParallel(spark, statements.slice(0, tier0End), "T0", sqlEc)

        if (statements.length > tier0End) {
          registerViewsParallel(spark, statements.slice(tier0End, statements.length), "T1", sqlEc)
        }
      } finally {
        sqlExecutor.shutdown()
      }

      // Read from landing view for landing table
      val returns_landing_final = {
        val returns_landing = spark.sql(s"select * from returns_landing")
        returns_landing.filter("product_number is not null")
          .drop("updated_datetime")
          .withColumn("req_tot_discounts", from_json(col("req_tot_discounts"), ArrayType(StringType)))
          .withColumn("return_act_discounts", from_json(col("return_act_discounts"), ArrayType(StringType)))
          .withColumn("lines_reasonCodeId", col("lines_reasonCodeId").cast(StringType))
          .withColumn(
            "order_datetime",
            date_format(
              to_timestamp(col("order_datetime"), "yyyy-MM-dd HH:mm:ss.SSS"),
              "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
            ).cast("string")
          )
          .withColumn(
            "return_date",
            date_format(
              to_timestamp(col("return_date"), "yyyy-MM-dd HH:mm:ss.SSS"),
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

      // Cache & materialize returns_refined DF eagerly
      val returns_refined = {
        val df = spark.sql(s"select * from returns")
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        val cnt = df.count()
        println(s"  returns_refined cached: $cnt rows")
        df
      }
      returnsRefinedDf = Some(returns_refined)

      val returns_refined_final = {
        val adjustmentSchema = ArrayType(
          StructType(Seq(
            StructField("adjustmentType", StringType),
            StructField("adjustmentTypeDesc", StringType),
            StructField("adjustmentTypeId", StringType),
            StructField("amount", StringType),
            StructField("override", StringType)
          ))
        )

        returns_refined.filter("product_number is not null")
          .withColumn("credits", from_json(col("credits"), adjustmentSchema))
          .withColumn("adjustments", from_json(col("adjustments"), adjustmentSchema))
          .withColumn("lines_reasonCodeId", col("lines_reasonCodeId").cast(StringType))
      }

      returns_refined_final.createOrReplaceTempView("returns_refined_final")

      // Pre-compute watermark while DF is cached
      val newMaxTs: Timestamp =
        Option(returns_refined.agg(max("load_time_adls").cast("timestamp")).as[Timestamp].first()).getOrElse(lastProcessedTs)

      executorService = Some(Executors.newFixedThreadPool(2))
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService.get)

      // Landing + Delta merge in parallel
      returns_landing_final.createOrReplaceTempView("returns_landing_merge")
      val landingFuture = Future {
		spark.sql(
        s"""
			MERGE INTO $return_landing_table t
			USING returns_landing_merge s
			ON s.order_id = t.order_id
				and s.return_id = t.return_id
				and s.company_number = t.company_number
				and s.lines_linenumber = t.lines_linenumber
				and s.return_status = t.return_status
				and t.ref_source = 'MAO'
				$mergeTargetPredicate
			WHEN MATCHED THEN UPDATE SET
				t.order_datetime = s.order_datetime,
				t.return_number = s.return_number,
				t.refund_method = s.refund_method,
				t.return_location = s.return_location,
				t.exchangeOrderNumber = s.exchangeOrderNumber,
				t.exchangeNumber = s.exchangeNumber,
				t.return_date = s.return_date,
				t.return_agent = s.return_agent,
				t.return_taxTransId = s.return_taxTransId,
				t.return_act_adjustmentAmount = s.return_act_adjustmentAmount,
				t.return_act_creditsAmount = s.return_act_creditsAmount,
				t.return_act_discountAmount = s.return_act_discountAmount,
				t.return_act_discountedTotalAmount = s.return_act_discountedTotalAmount,
				t.return_act_refundTotalAmount = s.return_act_refundTotalAmount,
				t.return_act_totalReturnAmount = s.return_act_totalReturnAmount,
				t.return_act_exchangeCreditAmount = s.return_act_exchangeCreditAmount,
				t.return_act_creditCardRefundAmount = s.return_act_creditCardRefundAmount,
				t.return_act_paypalRefundAmount = s.return_act_paypalRefundAmount,
				t.return_act_giftCardrefundAmount = s.return_act_giftCardrefundAmount,
				t.return_act_shippingAmount = s.return_act_shippingAmount,
				t.return_act_shippingTaxAmount = s.return_act_shippingTaxAmount,
				t.return_act_subTotalAmount = s.return_act_subTotalAmount,
				t.return_act_taxAmount = s.return_act_taxAmount,
				t.req_tot_adjustmentAmount = s.req_tot_adjustmentAmount,
				t.req_tot_creditsAmount = s.req_tot_creditsAmount,
				t.req_tot_discountAmount = s.req_tot_discountAmount,
				t.req_tot_discountedTotalAmount = s.req_tot_discountedTotalAmount,
				t.req_tot_refundTotalAmount = s.req_tot_refundTotalAmount,
				t.req_tot_totalReturnAmount = s.req_tot_totalReturnAmount,
				t.req_tot_exchangeCreditAmount = s.req_tot_exchangeCreditAmount,
				t.req_tot_creditCardRefundAmount = s.req_tot_creditCardRefundAmount,
				t.req_tot_paypalRefundAmount = s.req_tot_paypalRefundAmount,
				t.req_tot_giftCardrefundAmount = s.req_tot_giftCardrefundAmount,
				t.req_tot_shippingAmount = s.req_tot_shippingAmount,
				t.req_tot_shippingTaxAmount = s.req_tot_shippingTaxAmount,
				t.req_tot_subTotalAmount = s.req_tot_subTotalAmount,
				t.req_tot_taxAmount = s.req_tot_taxAmount,
				t.lines_refundNum = s.lines_refundNum,
				t.lines_act_priceOverrideReason = s.lines_act_priceOverrideReason,
				t.lines_act_discountAmount = s.lines_act_discountAmount,
				t.lines_act_discountedTotalAmount = s.lines_act_discountedTotalAmount,
				t.lines_act_originalRetailPrice = s.lines_act_originalRetailPrice,
				t.lines_act_shippingAmount = s.lines_act_shippingAmount,
				t.lines_act_shippingTaxAmount = s.lines_act_shippingTaxAmount,
				t.lines_act_giftBoxAmount = s.lines_act_giftBoxAmount,
				t.lines_act_giftBoxTaxAmount = s.lines_act_giftBoxTaxAmount,
				t.lines_act_subTotalAmount = s.lines_act_subTotalAmount,
				t.lines_act_taxAmount = s.lines_act_taxAmount,
				t.lines_act_totalAmount = s.lines_act_totalAmount,
				t.lines_act_unitPrice = s.lines_act_unitPrice,
				t.product_backorderFlag = s.product_backorderFlag,
				t.product_brand = s.product_brand,
				t.product_category = s.product_category,
				t.product_color = s.product_color,
				t.product_description = s.product_description,
				t.product_image = s.product_image,
				t.product_isCollectUpFront = s.product_isCollectUpFront,
				t.product_launchSkuFlag = s.product_launchSkuFlag,
				t.product_name = s.product_name,
				t.productDesignator = s.productDesignator,
				t.product_number = s.product_number,
				t.product_type = s.product_type,
				t.product_size = s.product_size,
				t.product_sku = s.product_sku,
				t.product_taxCode = s.product_taxCode,
				t.lines_qty = s.lines_qty,
				t.lines_reasonCode = s.lines_reasonCode,
				t.lines_reasonCodeId = s.lines_reasonCodeId,
				t.currencyIso = s.currencyIso,
				t.req_lines_priceOverrideReason = s.req_lines_priceOverrideReason,
				t.req_lines_discountAmount = s.req_lines_discountAmount,
				t.req_lines_discountedTotalAmount = s.req_lines_discountedTotalAmount,
				t.req_lines_originalRetailPrice = s.req_lines_originalRetailPrice,
				t.req_lines_shippingAmount = s.req_lines_shippingAmount,
				t.req_lines_shippingTaxAmount = s.req_lines_shippingTaxAmount,
				t.req_lines_subTotalAmount = s.req_lines_subTotalAmount,
				t.req_lines_taxAmount = s.req_lines_taxAmount,
				t.req_lines_totalAmount = s.req_lines_totalAmount,
				t.req_lines_giftBoxAmount = s.req_lines_giftBoxAmount,
				t.req_lines_giftBoxTaxAmount = s.req_lines_giftBoxTaxAmount,
				t.req_lines_unitPrice = s.req_lines_unitPrice,
				t.restockable = s.restockable,
				t.taxInvoiceNum = s.taxInvoiceNum,
				t.credits = s.credits,
				t.adjustments = s.adjustments,
				t.PaymentsInfo = s.PaymentsInfo,
				t.postedAt = s.postedAt,
				t.postedBy = s.postedBy,
				t.load_time_kafka = s.load_time_kafka,
				t.load_time_adls = s.load_time_adls,
				t.order_date = s.order_date,
				t.returningStore = s.returningStore,
				t.xstoreTransactionNumber = s.xstoreTransactionNumber,
				t.loyaltyDiscount = s.loyaltyDiscount,
				t.req_tot_discounts = s.req_tot_discounts,
				t.return_act_discounts = s.return_act_discounts,
				t.ref_source = s.ref_source
			WHEN NOT MATCHED THEN INSERT (order_id, order_datetime, return_id, company_number, return_status, return_number, refund_method, return_location, exchangeOrderNumber, exchangeNumber, return_date, return_agent, return_taxTransId, return_act_adjustmentAmount, return_act_creditsAmount, return_act_discountAmount, return_act_discountedTotalAmount, return_act_refundTotalAmount, return_act_totalReturnAmount, return_act_exchangeCreditAmount, return_act_creditCardRefundAmount, return_act_paypalRefundAmount, return_act_giftCardrefundAmount, return_act_shippingAmount, return_act_shippingTaxAmount, return_act_subTotalAmount, return_act_taxAmount, req_tot_adjustmentAmount, req_tot_creditsAmount, req_tot_discountAmount, req_tot_discountedTotalAmount, req_tot_refundTotalAmount, req_tot_totalReturnAmount, req_tot_exchangeCreditAmount, req_tot_creditCardRefundAmount, req_tot_paypalRefundAmount, req_tot_giftCardrefundAmount, req_tot_shippingAmount, req_tot_shippingTaxAmount, req_tot_subTotalAmount, req_tot_taxAmount, lines_refundNum, lines_act_priceOverrideReason, lines_act_discountAmount, lines_act_discountedTotalAmount, lines_act_originalRetailPrice, lines_act_shippingAmount, lines_act_shippingTaxAmount, lines_act_giftBoxAmount, lines_act_giftBoxTaxAmount, lines_act_subTotalAmount, lines_act_taxAmount, lines_act_totalAmount, lines_act_unitPrice, product_backorderFlag, product_brand, product_category, product_color, product_description, product_image, product_isCollectUpFront, product_launchSkuFlag, product_name, productDesignator, product_number, product_type, product_size, product_sku, product_taxCode, lines_qty, lines_reasonCode, lines_reasonCodeId, currencyIso, req_lines_priceOverrideReason, req_lines_discountAmount, req_lines_discountedTotalAmount, req_lines_originalRetailPrice, req_lines_shippingAmount, req_lines_shippingTaxAmount, req_lines_subTotalAmount, req_lines_taxAmount, req_lines_totalAmount, req_lines_giftBoxAmount, req_lines_giftBoxTaxAmount, req_lines_unitPrice, restockable, taxInvoiceNum, credits, adjustments, PaymentsInfo, postedAt, postedBy, load_time_kafka, load_time_adls, order_date, returningStore, xstoreTransactionNumber, loyaltyDiscount, req_tot_discounts, return_act_discounts, lines_lineNumber, ref_source)
			VALUES (s.order_id, s.order_datetime, s.return_id, s.company_number, s.return_status, s.return_number, s.refund_method, s.return_location, s.exchangeOrderNumber, s.exchangeNumber, s.return_date, s.return_agent, s.return_taxTransId, s.return_act_adjustmentAmount, s.return_act_creditsAmount, s.return_act_discountAmount, s.return_act_discountedTotalAmount, s.return_act_refundTotalAmount, s.return_act_totalReturnAmount, s.return_act_exchangeCreditAmount, s.return_act_creditCardRefundAmount, s.return_act_paypalRefundAmount, s.return_act_giftCardrefundAmount, s.return_act_shippingAmount, s.return_act_shippingTaxAmount, s.return_act_subTotalAmount, s.return_act_taxAmount, s.req_tot_adjustmentAmount, s.req_tot_creditsAmount, s.req_tot_discountAmount, s.req_tot_discountedTotalAmount, s.req_tot_refundTotalAmount, s.req_tot_totalReturnAmount, s.req_tot_exchangeCreditAmount, s.req_tot_creditCardRefundAmount, s.req_tot_paypalRefundAmount, s.req_tot_giftCardrefundAmount, s.req_tot_shippingAmount, s.req_tot_shippingTaxAmount, s.req_tot_subTotalAmount, s.req_tot_taxAmount, s.lines_refundNum, s.lines_act_priceOverrideReason, s.lines_act_discountAmount, s.lines_act_discountedTotalAmount, s.lines_act_originalRetailPrice, s.lines_act_shippingAmount, s.lines_act_shippingTaxAmount, s.lines_act_giftBoxAmount, s.lines_act_giftBoxTaxAmount, s.lines_act_subTotalAmount, s.lines_act_taxAmount, s.lines_act_totalAmount, s.lines_act_unitPrice, s.product_backorderFlag, s.product_brand, s.product_category, s.product_color, s.product_description, s.product_image, s.product_isCollectUpFront, s.product_launchSkuFlag, s.product_name, s.productDesignator, s.product_number, s.product_type, s.product_size, s.product_sku, s.product_taxCode, s.lines_qty, s.lines_reasonCode, s.lines_reasonCodeId, s.currencyIso, s.req_lines_priceOverrideReason, s.req_lines_discountAmount, s.req_lines_discountedTotalAmount, s.req_lines_originalRetailPrice, s.req_lines_shippingAmount, s.req_lines_shippingTaxAmount, s.req_lines_subTotalAmount, s.req_lines_taxAmount, s.req_lines_totalAmount, s.req_lines_giftBoxAmount, s.req_lines_giftBoxTaxAmount, s.req_lines_unitPrice, s.restockable, s.taxInvoiceNum, s.credits, s.adjustments, s.PaymentsInfo, s.postedAt, s.postedBy, s.load_time_kafka, s.load_time_adls, s.order_date, s.returningStore, s.xstoreTransactionNumber, s.loyaltyDiscount, s.req_tot_discounts, s.return_act_discounts, s.lines_lineNumber, s.ref_source)
       """)
      }

      val refinedFuture = Future {
        spark.sql(
        s"""
            MERGE INTO $return_refined_table t
            USING returns_refined_final s
            ON s.order_id = t.order_id 
				and t.company_number = s.company_number
				and t.return_number = s.return_number
				and t.lines_lineNumber = s.lines_lineNumber
				and t.ref_source = 'MAO'
				$mergeTargetPredicate
              WHEN MATCHED THEN UPDATE SET
				t.return_status = s.return_status,
				t.order_datetime = s.order_datetime,
				t.return_number = s.return_number,
				t.refund_method = s.refund_method,
				t.return_location = s.return_location,
				t.exchangeOrderNumber = s.exchangeOrderNumber,
				t.exchangeNumber = s.exchangeNumber,
				t.return_date = s.return_date,
				t.return_agent = s.return_agent,
				t.return_taxTransId = s.return_taxTransId,
				t.return_act_adjustmentAmount = s.return_act_adjustmentAmount,
				t.return_act_creditsAmount = s.return_act_creditsAmount,
				t.return_act_discountAmount = s.return_act_discountAmount,
				t.return_act_discountedTotalAmount = s.return_act_discountedTotalAmount,
				t.return_act_refundTotalAmount = s.return_act_refundTotalAmount,
				t.return_act_totalReturnAmount = s.return_act_totalReturnAmount,
				t.return_act_exchangeCreditAmount = s.return_act_exchangeCreditAmount,
				t.return_act_creditCardRefundAmount = s.return_act_creditCardRefundAmount,
				t.return_act_paypalRefundAmount = s.return_act_paypalRefundAmount,
				t.return_act_giftCardrefundAmount = s.return_act_giftCardrefundAmount,
				t.return_act_shippingAmount = s.return_act_shippingAmount,
				t.return_act_shippingTaxAmount = s.return_act_shippingTaxAmount,
				t.return_act_subTotalAmount = s.return_act_subTotalAmount,
				t.return_act_taxAmount = s.return_act_taxAmount,
				t.req_tot_adjustmentAmount = s.req_tot_adjustmentAmount,
				t.req_tot_creditsAmount = s.req_tot_creditsAmount,
				t.req_tot_discountAmount = s.req_tot_discountAmount,
				t.req_tot_discountedTotalAmount = s.req_tot_discountedTotalAmount,
				t.req_tot_refundTotalAmount = s.req_tot_refundTotalAmount,
				t.req_tot_totalReturnAmount = s.req_tot_totalReturnAmount,
				t.req_tot_exchangeCreditAmount = s.req_tot_exchangeCreditAmount,
				t.req_tot_creditCardRefundAmount = s.req_tot_creditCardRefundAmount,
				t.req_tot_paypalRefundAmount = s.req_tot_paypalRefundAmount,
				t.req_tot_giftCardrefundAmount = s.req_tot_giftCardrefundAmount,
				t.req_tot_shippingAmount = s.req_tot_shippingAmount,
				t.req_tot_shippingTaxAmount = s.req_tot_shippingTaxAmount,
				t.req_tot_subTotalAmount = s.req_tot_subTotalAmount,
				t.req_tot_taxAmount = s.req_tot_taxAmount,
				t.lines_refundNum = s.lines_refundNum,
				t.lines_act_priceOverrideReason = s.lines_act_priceOverrideReason,
				t.lines_act_discountAmount = s.lines_act_discountAmount,
				t.lines_act_discountedTotalAmount = s.lines_act_discountedTotalAmount,
				t.lines_act_originalRetailPrice = s.lines_act_originalRetailPrice,
				t.lines_act_shippingAmount = s.lines_act_shippingAmount,
				t.lines_act_shippingTaxAmount = s.lines_act_shippingTaxAmount,
				t.lines_act_giftBoxAmount = s.lines_act_giftBoxAmount,
				t.lines_act_giftBoxTaxAmount = s.lines_act_giftBoxTaxAmount,
				t.lines_act_subTotalAmount = s.lines_act_subTotalAmount,
				t.lines_act_taxAmount = s.lines_act_taxAmount,
				t.lines_act_totalAmount = s.lines_act_totalAmount,
				t.lines_act_unitPrice = s.lines_act_unitPrice,
				t.product_backorderFlag = s.product_backorderFlag,
				t.product_brand = s.product_brand,
				t.product_category = s.product_category,
				t.product_color = s.product_color,
				t.product_description = s.product_description,
				t.product_image = s.product_image,
				t.product_isCollectUpFront = s.product_isCollectUpFront,
				t.product_launchSkuFlag = s.product_launchSkuFlag,
				t.product_name = s.product_name,
				t.productDesignator = s.productDesignator,
				t.product_number = s.product_number,
				t.product_type = s.product_type,
				t.product_size = s.product_size,
				t.product_sku = s.product_sku,
				t.product_taxCode = s.product_taxCode,
				t.lines_qty = s.lines_qty,
				t.lines_reasonCode = s.lines_reasonCode,
				t.lines_reasonCodeId = s.lines_reasonCodeId,
				t.currencyIso = s.currencyIso,
				t.req_lines_priceOverrideReason = s.req_lines_priceOverrideReason,
				t.req_lines_discountAmount = s.req_lines_discountAmount,
				t.req_lines_discountedTotalAmount = s.req_lines_discountedTotalAmount,
				t.req_lines_originalRetailPrice = s.req_lines_originalRetailPrice,
				t.req_lines_shippingAmount = s.req_lines_shippingAmount,
				t.req_lines_shippingTaxAmount = s.req_lines_shippingTaxAmount,
				t.req_lines_subTotalAmount = s.req_lines_subTotalAmount,
				t.req_lines_taxAmount = s.req_lines_taxAmount,
				t.req_lines_totalAmount = s.req_lines_totalAmount,
				t.req_lines_giftBoxAmount = s.req_lines_giftBoxAmount,
				t.req_lines_giftBoxTaxAmount = s.req_lines_giftBoxTaxAmount,
				t.req_lines_unitPrice = s.req_lines_unitPrice,
				t.restockable = s.restockable,
				t.taxInvoiceNum = s.taxInvoiceNum,
				t.credits = s.credits,
				t.adjustments = s.adjustments,
				t.PaymentsInfo = s.PaymentsInfo,
				t.postedAt = s.postedAt,
				t.postedBy = s.postedBy,
				t.load_time_kafka = s.load_time_kafka,
				t.load_time_adls = s.load_time_adls,
				t.order_date = s.order_date,
				t.updated_datetime = s.updated_datetime,
				t.returningStore = s.returningStore,
				t.xstoreTransactionNumber = s.xstoreTransactionNumber,
				t.loyaltyDiscount = s.loyaltyDiscount,
				t.req_tot_discounts = s.req_tot_discounts,
				t.return_act_discounts = s.return_act_discounts,
				t.ref_source = s.ref_source
            WHEN NOT MATCHED THEN INSERT *
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

      println("Return load competed")

    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        updateAuditFailure(spark, auditTable, runId, s"Source table not found: ${ex.getMessage}")
        logError(s"Returns load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex

      case ex: Exception =>
        updateAuditFailure(spark, auditTable, runId, ex.getMessage)
        logError(s"Returns load failed with a general exception: ${ex.getMessage}", ex)
        throw ex

    } finally {
      returnsRefinedDf.foreach(_.unpersist())
      executorService.foreach(_.shutdown())
    }
  }
}
