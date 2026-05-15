package com.footlocker.oms_streaming.apps


import com.footlocker.conf.AppConfObject_Gen2.getSnowflakeConnectionOptions
import com.footlocker.services.SnowflakeHelperService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, parseUserAgentSimple, parseUserAgent_json, refreshPowerbi, udfFunc}
import com.footlocker.utils.Constants.AppendGen2ToSnowFlakeTablesFlag
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source

object AdlsOrders_Gen2 extends SnowflakeHelperService_Gen2 {


  def AdlsOrdersFunc(oms_order_adls: DataFrame, spark: SparkSession, inputParams : Map[String, String],products: DataFrame) {


    oms_order_adls.sparkSession.conf.set("spark.sql.shuffle.partitions", 2)
    oms_order_adls.sparkSession.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    // if (nofstreamsrunning(spark) != 6) {
    //   println("something is wrong")
    // }


    import spark.implicits._

    //FILTERING OUT ORDER MESSAGE
//    val oms_order_df = oms_order_adls.withColumn("orders_flag", when($"json".contains("""messageType":"ORDER"""), "1").when($"json".contains("""messageType" : "ORDER"""), "1").otherwise("0"))
//                          .filter("orders_flag == 1")
//                          .drop("orders_flag")
    val schemapath:String = (inputParams("schema_location")+"/orders.json")
    val parsed = applyschema(spark,oms_order_adls,schemapath)

    val parser = org.uaparser.scala.Parser.default
    lazy val parseUserAgentUDF = udf((userAgent: String) =>
    {
      parseUserAgent_json(parser,userAgent)
    })

    lazy val parseUserAgentSimpleUDF = udf((userAgent: String) =>
    {
      parseUserAgentSimple(parser ,userAgent)
    })
    val order_latest = parsed.select("oms_parsed_data.cancelCode",
      "oms_parsed_data.cancelReason",
      "oms_parsed_data.omsOrderId",
      "oms_parsed_data.postedBy",
      "oms_parsed_data.companyNumber",
      "oms_parsed_data.order",
      "oms_parsed_data.payment",
      "oms_parsed_data.orderId",
      "oms_parsed_data.orderStatus",
      "oms_parsed_data.postedAt",
      "load_time_kafka",
      "load_time_adls")

    val order_header_snowflake = order_latest
      .select(
        $"orderId" as "order_id",
        $"companyNumber" as "company_number",
        $"orderStatus" as "order_status",
        $"cancelCode" as "cancel_code",
        $"cancelReason",
        $"order.orderHeader.exchangeOrder" as "exchangeOrder_flag",
        $"order.orderHeader.billingAddress.addressLine1" as "billing_address_line1",
        $"order.orderHeader.billingAddress.addressLine2" as "billing_address_line2",
        trim($"order.orderHeader.billingAddress.city") as "billing_city",
        trim(upper($"order.orderHeader.billingAddress.country")) as "billing_country",
        trim(lower($"order.orderHeader.billingAddress.email")) as "billing_email",
        trim(lower($"order.orderHeader.billingAddress.firstName")) as "billing_first_name",
        trim(lower($"order.orderHeader.billingAddress.lastName")) as "billing_last_name",
        trim($"order.orderHeader.billingAddress.postalCode") as "billing_postal_code",
        trim(upper($"order.orderHeader.billingAddress.state")) as "billing_postal_state",
        trim(upper($"order.orderHeader.billingAddress.phoneNumber")) as "billing_phoneNumber",
        trim($"order.orderHeader.giftBoxFlag") as "order_giftbox_flag",
        trim($"order.orderHeader.giftOrderFlag") as "order_giftorder_flag",
        trim($"order.orderHeader.langID") as "order_langID",
        $"order.orderHeader.coupons" as "coupons",
        $"order.orderHeader.channel" as "channel",
        trim($"order.orderHeader.orderDateTime") as "order_datetime",
        trim($"order.orderHeader.baseShippingCharged") as "baseShippingCharged",
        trim($"order.orderHeader.shippableConsignmentCount") as "shippableConsignmentCount",
        trim($"order.orderHeader.referralSite") as "referralSite",
        trim($"order.orderHeader.orderPricing.baseShippingAmount") as "oh_base_shipping_amount",
        trim(upper($"order.orderHeader.orderPricing.currencyIso")) as "oh_currency",
        trim(upper($"order.orderHeader.orderPricing.couponAmount")) as "oh_couponamount",
        trim(upper($"order.orderHeader.orderPricing.giftBoxAmount")) as "oh_giftBoxAmount",
        trim(upper($"order.orderHeader.orderPricing.giftBoxTaxAmount")) as "oh_giftBoxTaxAmount",
        trim(upper($"order.orderHeader.orderPricing.baseShippingTaxAmount")) as "oh_baseShippingTaxAmount",
        trim(upper($"order.orderHeader.orderPricing.settledAmount")) as "oh_settledAmount",
        trim($"order.orderHeader.orderPricing.discountAmount") as "oh_discounted_amount",
        trim($"order.orderHeader.orderPricing.discountedTotalAmount") as "oh_discounted_total_amount",
        trim($"order.orderHeader.orderPricing.gateway") as "oh_gateway",
        trim($"order.orderHeader.orderPricing.shippingAmount") as "oh_shipping_amount",
        trim($"order.orderHeader.orderPricing.subTotalAmount") as "oh_subTotal_amount",
        trim($"order.orderHeader.orderPricing.taxAmount") as "oh_tax_amount",
        trim($"order.orderHeader.orderPricing.totalAmount") as "oh_total_amount",
        trim(lower($"order.user.email")) as "email",
        trim(lower($"order.user.firstName")) as "first_name",
        trim(lower($"order.user.lastName")) as "last_name",
        trim(lower($"order.user.id")) as "user_id",
        trim(lower($"order.user.type")) as "user_type",
        trim(lower($"order.user.controllerCustomerId")) as "controllerCustomerId",
        trim(lower($"order.user.relateCustomerId")) as "relateCustomerId",
        trim(lower($"order.user.flxId")) as "flxId",
        trim($"order.orderHeader.phoneNumber") as "order_phoneNumber",
        $"order.orderRequest.affiliateIdTime" as "order_affiliateIdTime",
        $"order.orderRequest.affiliateId" as "order_affiliate_id",
        trim($"order.orderRequest.flRequestId") as "order_flrequest_id",
        trim($"order.orderRequest.requestID") as "order_request_id",
        trim($"order.orderRequest.requestDate") as "order_request_date",
        $"order.orderRequest.requestType" as "order_request_type",
        $"order.orderRequest.requester" as "order_requester",
        trim(lower($"order.orderHeader.rushFlag")) as "rush_flag",
        trim($"order.orderHeader.salesPersonID") as "sales_personID",
        trim(lower($"order.orderHeader.shipMethod")) as "ship_method",
        trim(lower($"order.orderHeader.shipMethodDesc")) as "ship_method_desc",
        trim($"order.orderHeader.userIPAddress") as "user_ip_address",
        trim($"order.orderHeader.vendorId") as "vendorId",
        trim($"order.orderHeader.webOrderNumber") as "web_order_number",
        trim($"order.orderHeader.migratedOrder") as "migratedOrder",
        trim($"order.orderHeader.overrideReasonCd") as "order_overrideReasonCd",
        trim($"order.orderHeader.source") as "order_source",
        trim($"order.orderHeader.relatedOrderNumber") as "relatedordernumber_csa",
        trim($"order.orderHeader.controllerOrderNumber") as "controllerOrderNumber",
        trim(lower($"order.orderHeader.shippingAddress.addressLine1")) as "shipping_addressline1",
        trim(lower($"order.orderHeader.shippingAddress.addressLine2")) as "shipping_addressline2",
        trim(lower($"order.orderHeader.shippingAddress.city")) as "shipping_city",
        trim(upper($"order.orderHeader.shippingAddress.country")) as "shipping_country",
        trim(upper($"order.orderHeader.shippingAddress.state")) as "shipping_state",
        trim($"order.orderHeader.shippingAddress.postalCode") as "shipping_postal_code",
        trim(lower($"order.orderHeader.shippingAddress.email")) as "shipping_email",
        trim(lower($"order.orderHeader.shippingAddress.firstName")) as "shipping_first_name",
        trim(lower($"order.orderHeader.shippingAddress.lastName")) as "shipping_last_name",
        trim(lower($"order.orderHeader.shippingAddress.phoneNumber")) as "shipping_phoneNumber",
        trim(lower($"order.orderHeader.csaAgentId")) as "csaAgentId",
        trim(lower($"order.orderHeader.csaOrderNote")) as "csaOrderNote",
        trim(lower($"order.orderHeader.division")) as "order_division",
        $"omsOrderId",
        $"postedBy",
        $"postedAt",
        $"load_time_kafka",
        $"load_time_adls",
        $"order.orderHeader.appeasementOrder" as "appeasementOrder",
        $"order.orderHeader.noChargeOrder" as "noChargeOrder",
        udfFunc($"payment.authorizations",lit("paymentType")) as "payment_type",
        $"order.orderHeader.obfOrder" as "obfOrder",
        $"payment",
          ($"order.fullfillmentGrouping".getItem(0)("orderLines").getItem(0)("storeFulfillment")("storeAddress").as("storeAddress")),
        trim($"order.orderHeader.loyaltyDiscount") as "loyaltyDiscount",
        trim($"order.orderHeader.loyaltyPointsRedeemed") as "loyaltyPointsRedeemed",
        trim($"order.orderHeader.loyaltyRewardId") as "loyaltyRewardId",
        trim($"order.orderHeader.loyaltyRedemptionId") as "loyaltyRedemptionId"
      ).withColumn("ref_source", lit("OMS"))


    writeSnowflakeTable(getSnowflakeConnectionOptions(),
        getTableName("ORDER_HEADER", inputParams(AppendGen2ToSnowFlakeTablesFlag)),
        order_header_snowflake,
        SaveMode.Append)
println("write to order header done")
    val order_explode_1 = order_latest.select(
      trim(order_latest("orderId")) as "order_id",
      trim(upper(order_latest("orderStatus"))) as "order_status",
      order_latest("cancelCode") as "cancel_code",
      order_latest("cancelReason") as "cancelReason",
        order_latest("omsOrderId") as "omsOrderId",
          order_latest("postedBy") as "postedBy",
      trim(order_latest("companyNumber")) as "company_number",
      order_latest("order"),
      order_latest("payment"),
      explode_outer(order_latest("order.fullfillmentGrouping")).alias("fillfillment_grouping"),
      trim(order_latest("postedAt")) as "postedAt",
      $"load_time_kafka",
      $"load_time_adls"
    )


    val order_explode_2 = order_explode_1.select($"order_id",
      $"company_number",
      $"order",
      $"fillfillment_grouping",
      udfFunc($"payment.authorizations",lit("paymentType")) as "payment_type",
      explode_outer($"fillfillment_grouping.orderLines") as "order_lines",
      $"order_status",
      $"cancel_code",
      $"cancelReason",
      $"omsOrderId",
      $"postedBy",
      $"postedAt",
      $"load_time_kafka",
      $"load_time_adls"
    )

    //useragent devicetype new column pii layer
    val order_pii = order_explode_2
      .select(
        $"order_id",
        $"company_number",
        $"order_status",
        $"cancel_code",
        $"cancelReason",
        $"fillfillment_grouping.fullfillmentType" as "fullfillment_type",
        $"payment_type",
        $"order_lines.freeShipping" as "free_shipping",
        $"order_lines.lineNumber" as "order_lineNumber",
        $"order_lines.orderLinePricing.currencyIso" as "order_currency",
        $"order_lines.saleCode" as "order_salecode",
        $"order_lines.orderLinePricing.priceOverrideReason" as "order_priceOverrideReason",
        $"order_lines.orderLinePricing.discountAmount" as "order_discountAmount",
        $"order_lines.orderLinePricing.discountedTotalAmount" as "order_discounted_totalAmount",
        $"order_lines.orderLinePricing.originalRetailPrice" as "order_original_retailPrice",
        $"order_lines.orderLinePricing.shippingAmount" as "order_shippingAmount",
        $"order_lines.orderLinePricing.shippingTaxAmount" as "order_shippingTaxAmount",
        $"order_lines.orderLinePricing.subTotalAmount" as "order_subTotalAmount",
        $"order_lines.orderLinePricing.taxAmount" as "order_taxAmount",
        $"order_lines.orderLinePricing.giftBoxAmount" as "order_giftBoxAmount",
        $"order_lines.orderLinePricing.giftBoxTaxAmount" as "order_giftBoxTaxAmount",
        $"order_lines.orderLinePricing.totalAmount" as "order_totalAmount",
        $"order_lines.orderLinePricing.unitPrice" as "order_unitPrice",
        $"order_lines.giftCardNum" as "order_giftCardNum",
        $"order_lines.inventoryLocation" as "order_inventoryLocation",
        $"order_lines.product.name" as "order_product_name",
        $"order_lines.product.image" as "order_product_image",
        $"order_lines.product.backorderFlag" as "order_backorderFlag",
        $"order_lines.product.brand" as "order_product_brand",
        $"order_lines.product.category" as "order_product_category",
        $"order_lines.product.color" as "order_product_color",
        $"order_lines.product.description" as "order_product_description",
        $"order_lines.product.isCollectUpFront" as "order_product_isCollectUpFront",
        $"order_lines.product.launchSkuFlag" as "order_product_launch_SkuFlag",
        $"order_lines.product.productDesignator" as "order_product_designator",
        trim($"order_lines.product.productNumber") as "order_product_number",
        $"order_lines.product.productType" as "order_product_type",
        $"order_lines.product.size" as "order_product_size",
        $"order_lines.product.sku" as "order_product_sku",
        $"order_lines.product.taxCode" as "order_product_taxCode",
        $"order_lines.quantity" as "order_quantity",
        $"order_lines.s2s" as "s2s",
        $"order_lines.shipMethod" as "order_shipMethod",
        $"order_lines.taxCode" as "order_taxCode",
        $"order_lines.storeFulfillment.shipMethod" as "store_fulfillment_shipMethod",
        $"order_lines.storeFulfillment.storeNumber" as "store_fulfillment_storeNumber",
        $"order_lines.storeFulfillment.fulfillmentType" as "store_fulfillment_fulfillmentType",
        $"order_lines.storeFulfillment.pickupPersonEmail" as "store_fulfillment_pickupPersonEmail",
        $"order_lines.storeFulfillment.pickupPersonMobile" as "store_fulfillment_pickupPersonMobile",
        $"order_lines.storeFulfillment.storeCostOfGoods" as "store_fulfillment_storeCostOfGoods",
        $"order_lines.storeFulfillment.deliveryEstimateID" as "store_fulfillment_deliveryEstimateID",
        $"order_lines.storeFulfillment.deliveryInstructions" as "store_fulfillment_deliveryInstructions",
        $"order_lines.storeFulfillment.deliveryCustomerPhone" as "store_fulfillment_deliverycustomerphone",
        $"order_lines.orderLineReservations.lineId" as "lineId",
        $"order_lines.orderLineReservations.uom" as "uom",
        $"order_lines.orderLineReservations.productId" as "productId",
        $"order_lines.orderLineReservations.locationReservationDetails" as "locationReservationDetails",
        trim($"order.orderHeader.userAgent") as "userAgent",

        $"order.orderHeader.obfOrder" as "obfOrder",
        $"order.orderHeader.exchangeOrder" as "exchangeOrder_flag",
        $"order.orderHeader.billingAddress.addressLine1" as "billing_address_line1",
        $"order.orderHeader.billingAddress.addressLine2" as "billing_address_line2",
        trim($"order.orderHeader.billingAddress.city") as "billing_city",
        trim(upper($"order.orderHeader.billingAddress.country")) as "billing_country",
        trim(lower($"order.orderHeader.billingAddress.email")) as "billing_email",
        trim(lower($"order.orderHeader.billingAddress.firstName")) as "billing_first_name",
        trim(lower($"order.orderHeader.billingAddress.lastName")) as "billing_last_name",
        trim($"order.orderHeader.billingAddress.postalCode") as "billing_postal_code",
        trim(upper($"order.orderHeader.billingAddress.state")) as "billing_postal_state",
        trim(upper($"order.orderHeader.billingAddress.phoneNumber")) as "billing_phoneNumber",
        trim($"order.orderHeader.deviceID") as "order_deviceID",
        trim($"order.orderHeader.giftBoxFlag") as "order_giftbox_flag",
        trim($"order.orderHeader.giftOrderFlag") as "order_giftorder_flag",
        trim($"order.orderHeader.langID") as "order_langID",
        $"order.orderHeader.coupons" as "coupons",
        $"order.orderHeader.channel" as "channel",
        $"order.orderHeader.appeasementOrder" as "appeasementOrder",
        $"order.orderHeader.noChargeOrder" as "noChargeOrder",
        trim($"order.orderHeader.orderDateTime") as "order_datetime",
        trim($"order.orderHeader.baseShippingCharged") as "baseShippingCharged",
        trim($"order.orderHeader.shippableConsignmentCount") as "shippableConsignmentCount",
        trim($"order.orderHeader.referralSite") as "referralSite",
        trim($"order.orderHeader.orderPricing.baseShippingAmount") as "oh_base_shipping_amount",
        trim(upper($"order.orderHeader.orderPricing.currencyIso")) as "oh_currency",
        trim(upper($"order.orderHeader.orderPricing.couponAmount")) as "oh_couponamount",
        trim(upper($"order.orderHeader.orderPricing.giftBoxAmount")) as "oh_giftBoxAmount",
        trim(upper($"order.orderHeader.orderPricing.giftBoxTaxAmount")) as "oh_giftBoxTaxAmount",
        trim(upper($"order.orderHeader.orderPricing.baseShippingTaxAmount")) as "oh_baseShippingTaxAmount",
        trim(upper($"order.orderHeader.orderPricing.settledAmount")) as "oh_settledAmount",
        trim($"order.orderHeader.orderPricing.discountAmount") as "oh_discounted_amount",
        trim($"order.orderHeader.orderPricing.discountedTotalAmount") as "oh_discounted_total_amount",
        trim($"order.orderHeader.orderPricing.gateway") as "oh_gateway",
        trim($"order.orderHeader.orderPricing.shippingAmount") as "oh_shipping_amount",
        trim($"order.orderHeader.orderPricing.subTotalAmount") as "oh_subTotal_amount",
        trim($"order.orderHeader.orderPricing.taxAmount") as "oh_tax_amount",
        trim($"order.orderHeader.orderPricing.totalAmount") as "oh_total_amount",

        trim(lower($"order.user.email")) as "email",
        trim(lower($"order.user.firstName")) as "first_name",
        trim(lower($"order.user.lastName")) as "last_name",
        trim(lower($"order.user.id")) as "user_id",
        trim(lower($"order.user.type")) as "user_type",
        trim(lower($"order.user.controllerCustomerId")) as "controllerCustomerId",
        trim(lower($"order.user.relateCustomerId")) as "relateCustomerId",
        trim(lower($"order.user.flxId")) as "flxId",
        trim($"order.orderHeader.phoneNumber") as "order_phoneNumber",

        $"order.orderRequest.affiliateIdTime" as "order_affiliateIdTime",
        $"order.orderRequest.affiliateId" as "order_affiliate_id",
        trim($"order.orderRequest.flRequestId") as "order_flrequest_id",
        trim($"order.orderRequest.requestID") as "order_request_id",
        trim($"order.orderRequest.requestDate") as "order_request_date",
        $"order.orderRequest.requestType" as "order_request_type",
        $"order.orderRequest.requester" as "order_requester",
        trim(lower($"order.orderHeader.rushFlag")) as "rush_flag",
        trim($"order.orderHeader.salesPersonID") as "sales_personID",
        trim(lower($"order.orderHeader.shipMethod")) as "ship_method",
        trim(lower($"order.orderHeader.shipMethodDesc")) as "ship_method_desc",
        trim($"order.orderHeader.userIPAddress") as "user_ip_address",
        trim($"order.orderHeader.vendorId") as "vendorId",
        trim($"order.orderHeader.webOrderNumber") as "web_order_number",
        trim($"order.orderHeader.migratedOrder") as "migratedOrder",
        trim($"order.orderHeader.overrideReasonCd") as "order_overrideReasonCd",
        trim($"order.orderHeader.source") as "order_source",
        trim($"order.orderHeader.relatedOrderNumber") as "relatedordernumber_csa",
        trim($"order.orderHeader.controllerOrderNumber") as "controllerOrderNumber",
        trim(lower($"order.orderHeader.shippingAddress.addressLine1")) as "shipping_addressline1",
        trim(lower($"order.orderHeader.shippingAddress.addressLine2")) as "shipping_addressline2",
        trim(lower($"order.orderHeader.shippingAddress.city")) as "shipping_city",
        trim(upper($"order.orderHeader.shippingAddress.country")) as "shipping_country",
        trim(upper($"order.orderHeader.shippingAddress.state")) as "shipping_state",
        trim($"order.orderHeader.shippingAddress.postalCode") as "shipping_postal_code",
        trim(lower($"order.orderHeader.shippingAddress.email")) as "shipping_email",
        trim(lower($"order.orderHeader.shippingAddress.firstName")) as "shipping_first_name",
        trim(lower($"order.orderHeader.shippingAddress.lastName")) as "shipping_last_name",
        trim(lower($"order.orderHeader.shippingAddress.phoneNumber")) as "shipping_phoneNumber",
        trim(lower($"order.orderHeader.csaAgentId")) as "csaAgentId",
        trim(lower($"order.orderHeader.csaOrderNote")) as "csaOrderNote",
        trim(lower($"order.orderHeader.division")) as "order_division",
        $"omsOrderId",
        $"postedBy",
        $"postedAt",
        $"load_time_kafka",
        to_date(trim($"order.orderHeader.orderDateTime")).cast("string") as "order_date",
        $"load_time_adls",
          ($"order.fullfillmentGrouping".getItem(0)("orderLines").getItem(0)("storeFulfillment")("storeAddress").as("storeAddress")),
        $"order_lines.orderLinePricing.discounts" as "OrderLineDiscounts",
        trim($"order.orderHeader.loyaltyDiscount") as "loyaltyDiscount",
        trim($"order.orderHeader.loyaltyPointsRedeemed") as "loyaltyPointsRedeemed",
        trim($"order.orderHeader.loyaltyRewardId") as "loyaltyRewardId",
        trim($"order.orderHeader.loyaltyRedemptionId") as "loyaltyRedemptionId"
      ).as("df1").join(products.as("df2"), (trim($"df1.order_product_number") === trim($"df2.p_product_number") && trim($"df1.order_product_sku") === trim($"df2.sku")), "left").drop("p_product_number","sku").withColumn("cogs",($"cogs"*$"order_quantity"))
      .withColumn("user_agent_info",  parseUserAgentUDF(col("userAgent")))
      .withColumn("device_type", parseUserAgentSimpleUDF(col("userAgent")))


      // order_pii.persist()
      order_pii.createOrReplaceTempView("order_pii")

      order_pii.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("order_date").save(inputParams("pii_landing_path")+ "/sales/oms/orders")
    /*########### START WEEKLY VACUUM AND OPTIMIZE ###########*/
    import java.time.LocalDate
    import java.time.DayOfWeek
    import java.time.format.DateTimeFormatter

    val today = LocalDate.now().format(DateTimeFormatter.ISO_DATE)
    val weekDayNumber = LocalDate.now().getDayOfWeek.getValue
    val tablePath = inputParams("pii_landing_path")+ "/sales/oms/orders"

    println(s"checking history to run weekly vacuum and optimize for ${tablePath} on DATE ${today}, WEEKDAY ${weekDayNumber}")

    if (weekDayNumber == 1) {
      // Get operation history
      val historyDf = spark.sql(s"DESCRIBE HISTORY delta.`$tablePath`")

      // Check if any VACUUM has run today
      val vacuumToday = historyDf.filter("operation = 'VACUUM END'").filter(
        historyDf("timestamp").cast("date") === today
      ).count() == 0

      // Check if any VACUUM has run today
      val optimizeToday = historyDf.filter("operation = 'OPTIMIZE'").filter(
        historyDf("timestamp").cast("date") === today
      ).count() == 0

      //running vaccum
      if (optimizeToday) {
        println(s"Running OPTIMIZE on: $tablePath")
        spark.sql(s"OPTIMIZE delta.`$tablePath`")
      }
      else {
        println("Skipping VACUUM as it is already ran today")
      }

      //running optimize
      if (vacuumToday) {
        println(s"Running VACUUM on: $tablePath with retention 168 hours")
        spark.sql(s"VACUUM delta.`$tablePath` RETAIN 168 HOURS")
      }
      else {
        println("Skipping OPTIMIZE as it is already ran today")
      }
    } else {
      println("Skipping VACUUM & OPTIMIZE as it is not weekday")
    }
    /*########### WEEKLY VACUUM AND OPTIMIZE END ###########*/

      val orderslinesquery = Source.fromInputStream(AdlsOrders_Gen2.getClass.getResourceAsStream("/sqls/orderline.sql")).mkString
      val snowfalkeorderlines = oms_order_adls.sparkSession.sql(orderslinesquery)
        .withColumn("ref_source", lit("OMS"))
      writeSnowflakeTable(getSnowflakeConnectionOptions(),
          getTableName("ORDER_LINE", inputParams(AppendGen2ToSnowFlakeTablesFlag)),
          snowfalkeorderlines,
          SaveMode.Append)

      println("write to order line done")

      val oms_customers = oms_order_adls.sparkSession.sql(
          """select company_number,
                        first_name,
                        last_name,
                        user_type, order_phoneNumber,
                        billing_first_name,
                        billing_last_name,
                        user_id,
                        billing_address_line1,
                        billing_address_line2,
                        billing_city,
                        billing_postal_code,
                        billing_postal_state,
                        billing_country,billing_email,
                        billing_phoneNumber,
                        shipping_addressline1,
                        shipping_addressline2,
                        shipping_city,
                        shipping_country,
                        shipping_state,
                        shipping_postal_code,
                        shipping_email,
                        shipping_first_name,
                        shipping_last_name,
                        shipping_phoneNumber,
                        flxId,
                        current_date updated_date,
                        current_date load_date ,
                        row_number() over (partition by user_id order by order_datetime desc) as rnk
                        from order_pii""").withColumn("ref_source", lit("OMS"))
      .filter("rnk = 1")
      .drop("rnk")
    oms_customers.createOrReplaceTempView("oms_customers")


    oms_order_adls.sparkSession.sql(
      """Merge into customer_landing_gen2.oms_customers t
        |          USING oms_customers s
        |          ON s.user_id = t.user_id
        |          WHEN MATCHED THEN UPDATE SET
        |          t.company_number=s.company_number,
        |          t.first_name=s.first_name,
        |          t.last_name=s.last_name,
        |          t.user_type=s.user_type,
        |          t.order_phoneNumber=s.order_phoneNumber,
        |          t.billing_first_name=s.billing_first_name,
        |          t.billing_last_name=s.billing_last_name,
        |          t.billing_address_line1=s.billing_address_line1,
        |          t.billing_address_line2=s.billing_address_line2,
        |          t.billing_city=s.billing_city,
        |          t.billing_postal_code=s.billing_postal_code,
        |          t.billing_postal_state=s.billing_postal_state,
        |          t.billing_country=s.billing_country,
        |          t.billing_email=s.billing_email,
        |          t.billing_phoneNumber=s.billing_phoneNumber,
        |          t.shipping_addressline1=s.shipping_addressline1,
        |          t.shipping_addressline2=s.shipping_addressline2,
        |          t.shipping_city=s.shipping_city,
        |          t.shipping_country=s.shipping_country,
        |          t.shipping_state=s.shipping_state,
        |          t.shipping_postal_code=s.shipping_postal_code,
        |          t.shipping_email=s.shipping_email,
        |          t.shipping_first_name=s.shipping_first_name,
        |          t.shipping_last_name=s.shipping_last_name,
        |          t.shipping_phoneNumber=s.shipping_phoneNumber,
        |          t.flxId=s.flxId,
        |          t.updated_date=s.updated_date
        |          WHEN NOT MATCHED THEN INSERT *
        |""".stripMargin)



    val partitionWindow = Window.partitionBy($"company_number",$"order_id",$"order_lineNumber",$"fullfillment_type").orderBy($"postedAt".desc)

    val rownumber = row_number().over(partitionWindow)
    val order_pii_filter = order_pii.select($"*", rownumber as "rnum")
      .filter($"rnum" === "1")
      .drop("rnum")
      .withColumn("updated_datetime",col("load_time_adls")).withColumn("ref_source", lit("OMS"))
    order_pii_filter.createOrReplaceTempView("order_pii_filter")



//    val mergequery = Source.fromInputStream(AdlsOrders_Gen2.getClass.getResourceAsStream("/sqls/mergesnoworderline.sql")).mkString
//    snowflakequeryrun(mergequery,Options)


      val order_refined = order_pii_filter.drop("first_name","last_name","billing_address_line1","billing_address_line2","billing_first_name","billing_last_name"
        ,"order_deviceID","billing_phoneNumber","shipping_phoneNumber","store_fulfillment_deliveryInstructions","store_fulfillment_deliverycustomerphone",
        "user_ip_address","shipping_addressline1","shipping_addressline2","shipping_first_name","shipping_last_name",
        "order_phoneNumber","store_fulfillment_pickupPersonMobile")
      .withColumn("email",md5(upper(trim($"email"))))
      .withColumn("store_fulfillment_pickupPersonEmail",md5(upper(trim($"store_fulfillment_pickupPersonEmail"))))
      .withColumn("shipping_email",md5(upper(trim($"shipping_email"))))
      .withColumn("billing_email",md5(upper(trim($"billing_email"))))
    order_refined.createOrReplaceTempView("order_refined")

    //useragent and devicetype new columns in merge query npii table
    oms_order_adls.sparkSession.sql(s"""
          MERGE INTO sales_gen2.oms_orders t
          USING order_refined s
          ON s.order_id = t.order_id AND s.order_lineNumber = t.order_lineNumber and t.company_number = s.company_number and t.fullfillment_type = s.fullfillment_type
          WHEN MATCHED THEN UPDATE SET
          t.order_status=s.order_status,
          t.cancel_code=s.cancel_code,
          t.cancelReason = s.cancelReason,
          t.free_shipping=s.free_shipping,
          t.order_salecode = s.order_salecode,
          t.order_priceOverrideReason = s.order_priceOverrideReason,
          t.order_currency=s.order_currency,
          t.order_discountAmount=s.order_discountAmount,
          t.order_discounted_totalAmount=s.order_discounted_totalAmount,
          t.order_original_retailPrice=s.order_original_retailPrice,
          t.order_shippingAmount=s.order_shippingAmount,
          t.order_shippingTaxAmount = s.order_shippingTaxAmount,
          t.order_subTotalAmount=s.order_subTotalAmount,
          t.order_giftBoxAmount = s.order_giftBoxAmount,
          t.order_giftBoxTaxAmount = s.order_giftBoxTaxAmount,
          t.order_taxAmount=s.order_taxAmount,
          t.order_totalAmount=s.order_totalAmount,
          t.order_unitPrice=s.order_unitPrice,
          t.order_giftCardNum = s.order_giftCardNum,
          t.order_inventoryLocation = s.order_inventoryLocation,
          t.order_backorderFlag=s.order_backorderFlag,
          t.order_product_brand=s.order_product_brand,
          t.order_product_category=s.order_product_category,
          t.order_product_color=s.order_product_color,
          t.order_product_description=s.order_product_description,
          t.order_product_isCollectUpFront=s.order_product_isCollectUpFront,
          t.order_product_launch_SkuFlag=s.order_product_launch_SkuFlag,
          t.order_product_name=s.order_product_name,
          t.order_product_designator=s.order_product_designator,
          t.order_product_number=s.order_product_number,
          t.order_product_type=s.order_product_type,
          t.order_product_size=s.order_product_size,
          t.order_product_sku=s.order_product_sku,
          t.order_product_taxCode=s.order_product_taxCode,
          t.order_quantity=s.order_quantity,
          t.order_shipMethod=s.order_shipMethod,
          t.order_taxCode=s.order_taxCode,
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
          t.order_giftbox_flag=s.order_giftbox_flag,
          t.order_giftorder_flag=s.order_giftorder_flag,
          t.order_langID=s.order_langID,
          t.coupons=s.coupons,
          t.channel=s.channel  ,
          t.order_datetime=s.order_datetime,
          t.baseShippingCharged = s.baseShippingCharged,
          t.shippableConsignmentCount = s.shippableConsignmentCount,
          t.referralSite = s.referralSite,
          t.oh_base_shipping_amount=s.oh_base_shipping_amount,
          t.oh_currency=s.oh_currency,
          t.oh_couponamount = s.oh_couponamount,
          t.oh_giftBoxAmount = s.oh_giftBoxAmount,
          t.oh_giftBoxTaxAmount = s.oh_giftBoxTaxAmount,
          t.oh_baseShippingTaxAmount = s.oh_baseShippingTaxAmount,
          t.oh_settledAmount = s.oh_settledAmount,
          t.oh_discounted_amount=s.oh_discounted_amount,
          t.oh_discounted_total_amount=s.oh_discounted_total_amount,
          t.oh_gateway=s.oh_gateway,
          t.oh_shipping_amount=s.oh_shipping_amount,
          t.oh_subTotal_amount=s.oh_subTotal_amount,
          t.oh_tax_amount=s.oh_tax_amount,
          t.oh_total_amount=s.oh_total_amount,
          t.email= s.email,
          t.user_id=s.user_id,
          t.user_type=s.user_type,
          t.order_affiliateIdTime = s.order_affiliateIdTime,
          t.order_affiliate_id = s.order_affiliate_id,
          t.order_flrequest_id = s.order_flrequest_id,
          t.order_request_id=s.order_request_id,
          t.order_request_date=s.order_request_date,
          t.order_request_type=s.order_request_type,
          t.rush_flag=s.rush_flag,
          t.sales_personID=s.sales_personID,
          t.ship_method=s.ship_method,
          t.ship_method_desc=s.ship_method_desc,
          t.vendorId=s.vendorId,
          t.web_order_number=s.web_order_number,
          t.migratedOrder = s.migratedOrder,
          t.order_overrideReasonCd = s.order_overrideReasonCd,
          t.order_source = s.order_source,
          t.relatedordernumber_csa = s.relatedordernumber_csa,
          t.controllerOrderNumber = s.controllerOrderNumber,
          t.shipping_city=s.shipping_city,
          t.shipping_country=s.shipping_country,
          t.shipping_state=s.shipping_state,
          t.shipping_postal_code=s.shipping_postal_code,
          t.shipping_email = s.shipping_email,
          t.csaAgentId = s.csaAgentId,
          t.csaOrderNote = s.csaOrderNote,
          t.order_division = s.order_division,
          t.omsOrderId = s.omsOrderId,
          t.postedAt=s.postedAt,
          t.load_time_kafka=s.load_time_kafka,
          t.order_date=s.order_date,
          t.updated_datetime = s.load_time_adls,
          t.controllerCustomerId = s.controllerCustomerId,
          t.relateCustomerId = s.relateCustomerId,
          t.flxId = s.flxId,
          t.appeasementOrder = s.appeasementOrder,
          t.noChargeOrder = s.noChargeOrder,
          t.payment_type = s.payment_type,
          t.s2s=s.s2s,
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
        println("merge to orders table done")

// ------------------ PowerBi refresh------------------------------------------

  val openOrders_url = (inputParams("openOrders_url"))
  val open_orders_flag:String =(inputParams("refresh_flag"))

  if(open_orders_flag =="on"){
    val routes1 = Array(openOrders_url)
    refreshPowerbi(routes1)
  }


  //  order_pii.unpersist()
  }


}