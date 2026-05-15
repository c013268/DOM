package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp_Gen2
import com.footlocker.oms_streaming.apps.AdlsLoadMain_Gen2.sparkSession
import com.footlocker.services.ADLSService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, prepareDf, readSqlFile}
import com.footlocker.utils.Logging
import com.footlocker.utils.AuditUtils.{getEffectiveWatermark, insertAuditStartBatch, updateAuditSuccessBatch, updateAuditFailureBatch}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.footlocker.utils.Common_Gen2.{parseUserAgent_json, parseUserAgentSimple, processSubstitutions, stringToMap}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType, StructField}
import org.apache.spark.sql.functions.{col, from_json}
import java.sql.Timestamp
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


object AdlsOrders_DOM extends Logging{


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

  def dom_orders_load(spark: SparkSession ,order_refined_table: String ,order_landing_table: String,loadType: String, varSubstitutions: String): Unit = {

    val upperLoadType = Option(loadType).map(_.trim.toUpperCase).getOrElse("INCREMENTAL")

    val jobName_1 = "DOM_OMS_ORDERS_REFINE_LOAD"
    val targetTable_1 = "OMS_ORDERS_REFINED"
    val auditTable = "prod.etl_stats_npii.dom_oms_etl_audit"
    val runId_1 = s"${jobName_1}_${System.currentTimeMillis()}"
    val jobName_2 = "DOM_OMS_ORDERS_LANDING_LOAD"
    val targetTable_2 = "OMS_ORDERS_LANDING"
    val runId_2 = s"${jobName_2}_${System.currentTimeMillis()}"

    val lastProcessedTs_1 = getEffectiveWatermark(spark, auditTable, jobName_1, upperLoadType)
    val lastProcessedTs_2 = getEffectiveWatermark(spark, auditTable, jobName_2, upperLoadType)

    println(s"Last processed watermark for npii: $lastProcessedTs_1")
    println(s"Last processed watermark for landing: $lastProcessedTs_2")

    // ─── Watermark-driven lookback dates ───────────────────────────────
    // Use the earlier of both watermarks for a safe merge target predicate.
    val earliestWatermark = if (lastProcessedTs_1.before(lastProcessedTs_2)) lastProcessedTs_1 else lastProcessedTs_2
    val baseMap = stringToMap(varSubstitutions)
    val sourceLookbackDays = baseMap.get("source_lookback_days")
      .flatMap(v => scala.util.Try(v.trim.toInt).toOption)
      .getOrElse(10000)
    val mergeTargetBufferDays = 100

    val lookbackDate = earliestWatermark.toLocalDateTime.toLocalDate
      .minusDays(sourceLookbackDays)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val mergeTargetDate = earliestWatermark.toLocalDateTime.toLocalDate
      .minusDays(sourceLookbackDays + mergeTargetBufferDays)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val mergeTargetPredicate = s"and t.order_date >= '$mergeTargetDate'"
    println(s"  [MERGE] mergeTargetDate=$mergeTargetDate (lookbackDate=$lookbackDate - ${mergeTargetBufferDays}d buffer)")
    // ─── End Watermark-driven lookback dates ───────────────────────────

    // Batch audit start - single Delta commit
    insertAuditStartBatch(spark, auditTable, Seq(
      (jobName_1, targetTable_1, runId_1, upperLoadType),
      (jobName_2, targetTable_2, runId_2, upperLoadType)
    ))

    var orderPiiDf: Option[DataFrame] = None
    var executorService: Option[ExecutorService] = None

    try {

      import spark.implicits._

      println("Orders Processing")


      // Define and register the UDF for parsing user agent strings
      val parser = org.uaparser.scala.Parser.default
      val parseUserAgentUDF = (userAgent: String, returnType: String) => {
        if (userAgent == null) {
          null
        } else {
          returnType.toLowerCase match {
            case "info" => parseUserAgent_json(parser, userAgent)
            case "type" => parseUserAgentSimple(parser, userAgent)
            case _ => null
          }
        }
      }
      spark.udf.register("parse_user_agent", parseUserAgentUDF)
      logInfo("Registered 'parse_user_agent' UDF.")

      val sqlFileName = "sqls/DOM-OMS_ORDERS.sql"
      val statements = {
        logInfo(s"Reading SQL from resource file: $sqlFileName")
        val rawSql = readSqlFile(sqlFileName)

        println(s"  [SUB] lookback_date=$lookbackDate (source_lookback_days=$sourceLookbackDays, lastProcessedTs=$lastProcessedTs_1)")
        val substitutionMap = baseMap ++ Map(
          "lookback_date" -> lookbackDate
        )
        val sourceSql = processSubstitutions(rawSql, substitutionMap)
        val stmts = sourceSql.split(";").map(_.trim).filter(_.nonEmpty)
        stmts.zipWithIndex.foreach { case (s, i) => println(s"  Parsed SQL[$i]: ${extractViewName(s)}") }
        stmts
      }

      // -------------------------------------------------------
      // PARALLEL SQL EXECUTION (tiered by view dependencies)
      // -------------------------------------------------------
      val sqlExecutor = Executors.newFixedThreadPool(2)
      val sqlEc: ExecutionContext = ExecutionContext.fromExecutorService(sqlExecutor)
      try {
        val tier0End = Math.min(2, statements.length)
        registerViewsParallel(spark, statements.slice(0, tier0End), "T0", sqlEc)

        if (statements.length > tier0End) {
          registerViewsParallel(spark, statements.slice(tier0End, statements.length), "T1", sqlEc)
        }
      } finally {
        sqlExecutor.shutdown()
      }

      // ──────────────────────────────────────────────────────────────────
      // Cache order_pii (used twice: refined merge + watermark).
      // orders_landing is NOT cached — used only once for merge.
      // ──────────────────────────────────────────────────────────────────

      // Cache + compute watermark for order_pii in a single action
      val (order_pii, newMaxTs_1) = {
        val df = spark.sql(s"select * from orders")
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        val stats = df.agg(
          count(lit(1)).as("cnt"),
          max(col("load_time_adls").cast("timestamp")).as("max_ts")
        ).head()
        val cnt = stats.getLong(0)
        val maxTs = Option(stats.getAs[Timestamp](1)).getOrElse(lastProcessedTs_1)
        println(s"  order_pii cached: $cnt rows, maxTs=$maxTs")
        (df, maxTs)
      }

      orderPiiDf = Some(order_pii)
      order_pii.createOrReplaceTempView("order_pii")

      // Lightweight watermark for landing: single-column aggregation on the view
      // Spark applies column pruning — only reads etl_updt_ts through the joins
      val newMaxTs_2 = {
        val ts = spark.sql("select max(cast(load_time_adls as timestamp)) from orders_landing").as[Timestamp].first()
        Option(ts).getOrElse(lastProcessedTs_2)
      }
      println(s"  landing watermark: $newMaxTs_2")

      val order_refined = {
        order_pii.filter("order_product_number is not null")
          .withColumn("coupons", from_json(col("coupons"), ArrayType(StringType)))
          .withColumn("email", md5(upper(trim($"email"))))
          .withColumn("store_fulfillment_pickupPersonEmail", md5(upper(trim($"store_fulfillment_pickupPersonEmail"))))
          .withColumn("shipping_email", md5(upper(trim($"shipping_email"))))
          .withColumn("billing_email", md5(upper(trim($"billing_email"))))
      }
      order_refined.createOrReplaceTempView("order_refined")

      // Prepare landing DF — no caching, used only once for merge
      // View is read directly during merge execution with Iceberg predicate pushdown
      val ordersLandingDf = {
        spark.sql(s"select * from orders_landing")
          .filter("order_product_number is not null")
          .withColumn("coupons", from_json(col("coupons"), ArrayType(StringType)))
      }
      ordersLandingDf.createOrReplaceTempView("orders_landing_merge")

      executorService = Some(Executors.newFixedThreadPool(2))
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService.get)

      // Refined merge + Landing write in parallel
      val refinedFuture = Future {
        println("Writing into orders refined table")
        spark.sql(
          s"""
            MERGE INTO $order_refined_table t
            USING order_refined s
            ON s.order_id = t.order_id
				and s.order_lineNumber = t.order_lineNumber
				and s.company_number = t.company_number
				$mergeTargetPredicate
            WHEN MATCHED THEN UPDATE SET
				t.order_status = s.order_status,
				t.cancel_code = s.cancel_code,
				t.cancelReason = s.cancelReason,
				t.free_shipping = s.free_shipping,
				t.order_salecode = s.order_salecode,
				t.order_priceOverrideReason = s.order_priceOverrideReason,
				t.order_currency = s.order_currency,
				t.order_discountAmount = s.order_discountAmount,
				t.order_discounted_totalAmount = s.order_discounted_totalAmount,
				t.order_original_retailPrice = s.order_original_retailPrice,
				t.order_shippingAmount = s.order_shippingAmount,
				t.order_shippingTaxAmount = s.order_shippingTaxAmount,
				t.order_subTotalAmount = s.order_subTotalAmount,
				t.order_giftBoxAmount = s.order_giftBoxAmount,
				t.order_giftBoxTaxAmount = s.order_giftBoxTaxAmount,
				t.order_taxAmount = s.order_taxAmount,
				t.order_totalAmount = s.order_totalAmount,
				t.order_unitPrice = s.order_unitPrice,
				t.order_giftCardNum = s.order_giftCardNum,
				t.order_inventoryLocation = s.order_inventoryLocation,
				t.order_backorderFlag = s.order_backorderFlag,
				t.order_product_brand = s.order_product_brand,
				t.order_product_category = s.order_product_category,
				t.order_product_color = s.order_product_color,
				t.order_product_description = s.order_product_description,
				t.order_product_isCollectUpFront = s.order_product_isCollectUpFront,
				t.order_product_launch_SkuFlag = s.order_product_launch_SkuFlag,
				t.order_product_name = s.order_product_name,
				t.order_product_designator = s.order_product_designator,
				t.order_product_number = s.order_product_number,
				t.order_product_type = s.order_product_type,
				t.order_product_size = s.order_product_size,
				t.order_product_sku = s.order_product_sku,
				t.order_product_taxCode = s.order_product_taxCode,
				t.order_quantity = s.order_quantity,
				t.order_shipMethod = s.order_shipMethod,
				t.order_taxCode = s.order_taxCode,
				t.store_fulfillment_shipMethod = s.store_fulfillment_shipMethod,
				t.store_fulfillment_storeNumber = s.store_fulfillment_storeNumber,
				t.store_fulfillment_fulfillmentType = s.store_fulfillment_fulfillmentType,
				t.store_fulfillment_pickupPersonEmail = s.store_fulfillment_pickupPersonEmail,
				t.store_fulfillment_storeCostOfGoods = s.store_fulfillment_storeCostOfGoods,
				t.store_fulfillment_deliveryEstimateID = s.store_fulfillment_deliveryEstimateID,
				t.exchangeOrder_flag = s.exchangeOrder_flag,
				t.billing_city = s.billing_city,
				t.billing_country = s.billing_country,
				t.billing_email = s.billing_email,
				t.billing_postal_code = s.billing_postal_code,
				t.billing_postal_state = s.billing_postal_state,
				t.order_giftbox_flag = s.order_giftbox_flag,
				t.order_giftorder_flag = s.order_giftorder_flag,
				t.order_langID = s.order_langID,
				t.coupons = s.coupons,
				t.channel = s.channel,
				t.order_datetime = s.order_datetime,
				t.baseShippingCharged = s.baseShippingCharged,
				t.shippableConsignmentCount = s.shippableConsignmentCount,
				t.referralSite = s.referralSite,
				t.oh_base_shipping_amount = s.oh_base_shipping_amount,
				t.oh_currency = s.oh_currency,
				t.oh_couponamount = s.oh_couponamount,
				t.oh_giftBoxAmount = s.oh_giftBoxAmount,
				t.oh_giftBoxTaxAmount = s.oh_giftBoxTaxAmount,
				t.oh_baseShippingTaxAmount = s.oh_baseShippingTaxAmount,
				t.oh_settledAmount = s.oh_settledAmount,
				t.oh_discounted_amount = s.oh_discounted_amount,
				t.oh_discounted_total_amount = s.oh_discounted_total_amount,
				t.oh_gateway = s.oh_gateway,
				t.oh_shipping_amount = s.oh_shipping_amount,
				t.oh_subTotal_amount = s.oh_subTotal_amount,
				t.oh_tax_amount = s.oh_tax_amount,
				t.oh_total_amount = s.oh_total_amount,
				t.email = s.email,
				t.user_id = s.user_id,
				t.user_type = s.user_type,
				t.order_affiliateIdTime = s.order_affiliateIdTime,
				t.order_affiliate_id = s.order_affiliate_id,
				t.order_flrequest_id = s.order_flrequest_id,
				t.order_request_id = s.order_request_id,
				t.order_request_date = s.order_request_date,
				t.order_request_type = s.order_request_type,
				t.rush_flag = s.rush_flag,
				t.sales_personID = s.sales_personID,
				t.ship_method = s.ship_method,
			  t.ship_method_desc = s.ship_method_desc,
				t.vendorId = s.vendorId,
				t.web_order_number = s.web_order_number,
				t.migratedOrder = s.migratedOrder,
				t.order_overrideReasonCd = s.order_overrideReasonCd,
				t.order_source = s.order_source,
				t.relatedordernumber_csa = s.relatedordernumber_csa,
				t.controllerOrderNumber = s.controllerOrderNumber,
				t.shipping_city = s.shipping_city,
				t.shipping_country = s.shipping_country,
				t.shipping_state = s.shipping_state,
				t.shipping_postal_code = s.shipping_postal_code,
				t.shipping_email = s.shipping_email,
				t.csaAgentId = s.csaAgentId,
				t.csaOrderNote = s.csaOrderNote,
				t.order_division = s.order_division,
				t.omsOrderId = s.omsOrderId,
				t.postedAt = s.postedAt,
				t.load_time_kafka = s.load_time_kafka,
				t.order_date = s.order_date,
				t.updated_datetime = s.load_time_adls,
				t.controllerCustomerId = s.controllerCustomerId,
				t.relateCustomerId = s.relateCustomerId,
				t.flxId = s.flxId,
				t.appeasementOrder = s.appeasementOrder,
				t.noChargeOrder = s.noChargeOrder,
				t.payment_type = s.payment_type,
				t.s2s = s.s2s,
				t.lineId = s.lineId,
				t.uom = s.uom,
				t.productId = s.productId,
				t.locationReservationDetails = s.locationReservationDetails,
				t.cogs = s.cogs,
				t.obfOrder = s.obfOrder,
				t.storeAddress = s.storeAddress,
				t.OrderLineDiscounts = s.OrderLineDiscounts,
				t.loyaltyDiscount = s.loyaltyDiscount,
				t.loyaltyPointsRedeemed = s.loyaltyPointsRedeemed,
				t.loyaltyRewardId = s.loyaltyRewardId,
				t.loyaltyRedemptionId = s.loyaltyRedemptionId,
				t.userAgent = s.userAgent,
				t.user_agent_info = s.user_agent_info,
				t.device_type = s.device_type,
				t.ref_source = s.ref_source
            WHEN NOT MATCHED THEN INSERT *
               """)
      }

      val landingFuture = Future {
        println("Writing into orders landing table")
	  spark.sql(
        s"""
			MERGE INTO $order_landing_table t
			USING orders_landing_merge s
			ON s.order_id = t.order_id
				and s.company_number = t.company_number
				and s.order_status = t.order_status
				and s.order_linenumber = t.order_linenumber
				and t.ref_source = 'MAO'
				$mergeTargetPredicate
			WHEN MATCHED THEN UPDATE SET
				t.cancel_code = s.cancel_code,
				t.cancelReason = s.cancelReason,
				t.fullfillment_type = s.fullfillment_type,
				t.payment_type = s.payment_type,
				t.free_shipping = s.free_shipping,
				t.order_currency = s.order_currency,
				t.order_salecode = s.order_salecode,
				t.order_priceoverridereason = s.order_priceoverridereason,
				t.order_discountamount = s.order_discountamount,
				t.order_discounted_totalamount = s.order_discounted_totalamount,
				t.order_original_retailprice = s.order_original_retailprice,
				t.order_shippingamount = s.order_shippingamount,
				t.order_shippingtaxamount = s.order_shippingtaxamount,
				t.order_subtotalamount = s.order_subtotalamount,
				t.order_taxamount = s.order_taxamount,
				t.order_giftboxamount = s.order_giftboxamount,
				t.order_giftboxtaxamount = s.order_giftboxtaxamount,
				t.order_totalamount = s.order_totalamount,
				t.order_unitprice = s.order_unitprice,
				t.order_giftCardNum = s.order_giftCardNum,
				t.order_inventoryLocation = s.order_inventoryLocation,
				t.order_product_name = s.order_product_name,
				t.order_product_image = s.order_product_image,
				t.order_backorderFlag = s.order_backorderFlag,
				t.order_product_brand = s.order_product_brand,
				t.order_product_category = s.order_product_category,
				t.order_product_color = s.order_product_color,
				t.order_product_description = s.order_product_description,
				t.order_product_isCollectUpFront = s.order_product_isCollectUpFront,
				t.order_product_launch_SkuFlag = s.order_product_launch_SkuFlag,
				t.order_product_designator = s.order_product_designator,
				t.order_product_number = s.order_product_number,
				t.order_product_type = s.order_product_type,
				t.order_product_size = s.order_product_size,
				t.order_product_sku = s.order_product_sku,
				t.order_product_taxCode = s.order_product_taxCode,
				t.order_quantity = s.order_quantity,
				t.order_shipMethod = s.order_shipMethod,
				t.order_taxCode = s.order_taxCode,
				t.store_fulfillment_shipMethod = s.store_fulfillment_shipMethod,
				t.store_fulfillment_storeNumber = s.store_fulfillment_storeNumber,
				t.store_fulfillment_fulfillmentType = s.store_fulfillment_fulfillmentType,
				t.store_fulfillment_pickupPersonEmail = s.store_fulfillment_pickupPersonEmail,
				t.store_fulfillment_pickupPersonMobile = s.store_fulfillment_pickupPersonMobile,
				t.store_fulfillment_storeCostOfGoods = s.store_fulfillment_storeCostOfGoods,
				t.store_fulfillment_deliveryEstimateId = s.store_fulfillment_deliveryEstimateId,
				t.store_fulfillment_deliveryInstructions = s.store_fulfillment_deliveryInstructions,
				t.store_fulfillment_deliverycustomerphone = s.store_fulfillment_deliverycustomerphone,
				t.exchangeOrder_flag = s.exchangeOrder_flag,
				t.billing_address_line1 = s.billing_address_line1,
				t.billing_address_line2 = s.billing_address_line2,
				t.billing_city = s.billing_city,
				t.billing_country = s.billing_country,
				t.billing_email = s.billing_email,
				t.billing_first_name = s.billing_first_name,
				t.billing_last_name = s.billing_last_name,
				t.billing_postal_code = s.billing_postal_code,
				t.billing_postal_state = s.billing_postal_state,
				t.billing_phoneNumber = s.billing_phoneNumber,
				t.order_deviceID = s.order_deviceID,
				t.order_giftbox_flag = s.order_giftbox_flag,
				t.order_giftorder_flag = s.order_giftorder_flag,
				t.order_langid = s.order_langid,
				t.coupons = s.coupons,
				t.channel = s.channel,
				t.appeasementorder = s.appeasementorder,
				t.noChargeOrder = s.noChargeOrder,
				t.order_datetime = s.order_datetime,
				t.baseShippingCharged = s.baseShippingCharged,
				t.shippableConsignmentCount = s.shippableConsignmentCount,
				t.referralSite = s.referralSite,
				t.oh_base_shipping_amount = s.oh_base_shipping_amount,
				t.oh_currency = s.oh_currency,
				t.oh_couponamount = s.oh_couponamount,
				t.oh_giftBoxAmount = s.oh_giftBoxAmount,
				t.oh_giftBoxTaxAmount = s.oh_giftBoxTaxAmount,
				t.oh_baseShippingTaxAmount = s.oh_baseShippingTaxAmount,
				t.oh_settledAmount = s.oh_settledAmount,
				t.oh_discounted_amount = s.oh_discounted_amount,
				t.oh_discounted_total_amount = s.oh_discounted_total_amount,
				t.oh_gateway = s.oh_gateway,
				t.oh_shipping_amount = s.oh_shipping_amount,
				t.oh_subTotal_amount = s.oh_subTotal_amount,
				t.oh_tax_amount = s.oh_tax_amount,
				t.oh_total_amount = s.oh_total_amount,
				t.email = s.email,
				t.first_name = s.first_name,
				t.last_name = s.last_name,
				t.user_id = s.user_id,
				t.user_type = s.user_type,
				t.controllercustomerid = s.controllercustomerid,
				t.relatecustomerid = s.relatecustomerid,
				t.flxid = s.flxid,
				t.order_phoneNumber = s.order_phoneNumber,
				t.order_affiliateIdTime = s.order_affiliateIdTime,
				t.order_affiliate_id = s.order_affiliate_id,
				t.order_flrequest_id = s.order_flrequest_id,
				t.order_request_id = s.order_request_id,
				t.order_request_date = s.order_request_date,
				t.order_request_type = s.order_request_type,
				t.order_requester = s.order_requester,
				t.rush_flag = s.rush_flag,
				t.sales_personID = s.sales_personID,
				t.ship_method = s.ship_method,
				t.ship_method_desc = s.ship_method_desc,
				t.user_ip_address = s.user_ip_address,
				t.vendorId = s.vendorId,
				t.web_order_number = s.web_order_number,
				t.migratedOrder = s.migratedOrder,
				t.order_overrideReasoncd = s.order_overrideReasoncd,
				t.order_source = s.order_source,
				t.relatedordernumber_csa = s.relatedordernumber_csa,
				t.controllerOrderNumber = s.controllerOrderNumber,
				t.shipping_addressline1 = s.shipping_addressline1,
				t.shipping_addressline2 = s.shipping_addressline2,
				t.shipping_city = s.shipping_city,
				t.shipping_country = s.shipping_country,
				t.shipping_state = s.shipping_state,
				t.shipping_postal_code = s.shipping_postal_code,
				t.shipping_email = s.shipping_email,
				t.shipping_first_name = s.shipping_first_name,
				t.shipping_last_name = s.shipping_last_name,
				t.shipping_phoneNumber = s.shipping_phoneNumber,
				t.csaAgentId = s.csaAgentId,
				t.csaOrderNote = s.csaOrderNote,
				t.order_division = s.order_division,
				t.omsOrderId = s.omsOrderId,
				t.postedBy = s.postedBy,
				t.postedAt = s.postedAt,
				t.load_time_kafka = s.load_time_kafka,
				t.order_date = s.order_date,
				t.load_time_adls = s.load_time_adls,
				t.cogs = s.cogs,
				t.s2s = s.s2s,
				t.lineId = s.lineId,
				t.uom = s.uom,
				t.productId = s.productId,
				t.obfOrder = s.obfOrder,
				t.locationReservationDetails = s.locationReservationDetails,
				t.storeAddress = s.storeAddress,
				t.OrderLineDiscounts = s.OrderLineDiscounts,
				t.loyaltyDiscount = s.loyaltyDiscount,
				t.loyaltyPointsRedeemed = s.loyaltyPointsRedeemed,
				t.loyaltyRewardId = s.loyaltyRewardId,
				t.loyaltyRedemptionId = s.loyaltyRedemptionId,
				t.userAgent = s.userAgent,
				t.user_agent_info = s.user_agent_info,
				t.device_type = s.device_type,
				t.ref_source = s.ref_source
			WHEN NOT MATCHED THEN INSERT *
       """)
      }

      // Wait for all parallel paths
      val mergeResult   = scala.util.Try(Await.result(refinedFuture, 4.hours))
      val landingResult = scala.util.Try(Await.result(landingFuture, 4.hours))

      (mergeResult, landingResult) match {
        case (scala.util.Failure(ex), _) => throw ex
        case (_, scala.util.Failure(ex)) => throw ex
        case _ => // all succeeded
      }

      // Batch audit success - single Delta commit
      updateAuditSuccessBatch(spark, auditTable, Seq(
        runId_1 -> newMaxTs_1,
        runId_2 -> newMaxTs_2
      ))

      println("Orders load competed")
    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        updateAuditFailureBatch(spark, auditTable, Seq(runId_1, runId_2), s"Source table not found: ${ex.getMessage}")
        logError(s"Orders load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex

      case ex: Exception =>
        updateAuditFailureBatch(spark, auditTable, Seq(runId_1, runId_2), ex.getMessage)
        logError(s"Orders load failed with a general exception: ${ex.getMessage}", ex)
        throw ex

    } finally {
      orderPiiDf.foreach(_.unpersist())
      executorService.foreach(_.shutdown())
    }
  }
}
