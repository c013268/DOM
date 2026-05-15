package com.footlocker.oms_streaming.apps

import com.footlocker.conf.AppConfObject_Gen2.getSnowflakeConnectionOptions
import com.footlocker.services.SnowflakeHelperService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, refreshPowerbi, udfFunc}
import com.footlocker.utils.Constants.AppendGen2ToSnowFlakeTablesFlag
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object AdlsConsignment_Gen2 extends SnowflakeHelperService_Gen2 {

  def consignment_adls(oms_consignment_adls: DataFrame, spark : SparkSession,inputParams : Map[String, String]) {
    oms_consignment_adls.sparkSession.conf.set("spark.sql.shuffle.partitions", 2)
    oms_consignment_adls.sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    oms_consignment_adls.sparkSession.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    //READING SCHEMA FILE
    import spark.implicits._

    //FILTERING OUT CONSIGNMENT MESSAGE
//    val oms_consignment_df = oms_consignment_adls.withColumn("consignment_flag", when($"json".contains("""messageType":"CONSIGNMENT"""), "1").when($"json".contains("""messageType" : "CONSIGNMENT"""), "1").otherwise("0"))
//      .filter("consignment_flag == 1")
//      .drop("consignment_flag")
    val open_orders_flag:String =(inputParams("refresh_flag"))
    val schemapath:String = (inputParams("schema_location")+"/consignments.json")
    val parsed = applyschema(spark,oms_consignment_adls,schemapath)




    val consignment_latest = parsed.select("oms_parsed_data.orderid",
      "oms_parsed_data.order",
      "oms_parsed_data.consignment",
      "oms_parsed_data.postedAt",
      "load_time_kafka",
      "load_time_adls")


    val consignment_explode_1 = consignment_latest.select(
      trim(consignment_latest("orderId")) as "order_id",
      consignment_latest("order") as "order",
      $"order.orderHeader.companyNumber" as "company_number",
      $"order.orderHeader.orderDateTime" as "orderDateTime",
      consignment_latest("consignment"),
      consignment_latest("consignment.consignmentEntries").alias("consignmentEntries"),
      $"consignment.paymentsInfo" as "payments_info",
      $"consignment.presell" as "presell",
      trim(consignment_latest("postedAt")) as "postedAt",
      $"load_time_kafka",
      $"load_time_adls"
    )

    //consignment_explode_1.persist()

    val consignment_snow = consignment_explode_1
      .select(
        $"order_id",
        $"order.orderHeader.companyNumber" as "company_number",
        $"order.user.id" as "cust_id",
        $"order.user.relateCustomerId" as "relateCustomerId",
        $"order.user.flxId" as "flxId",
        concat($"order.user.firstName",lit(" "),$"order.user.lastName") as "Customername",
        trim(lower($"order.user.email")) as "email",
        trim($"order.orderHeader.phoneNumber") as "order_phoneNumber",
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
        trim(lower($"order.orderHeader.shipMethod")) as "ship_method",
        trim(lower($"order.orderHeader.shipMethodDesc")) as "ship_method_desc",
        ($"order.orderHeader.orderPricing.subTotalAmount" - $"order.orderHeader.orderPricing.discountAmount") as "OrderTotalAmount",
        $"consignment.consignmentId" as "consignment_Id",
        $"consignment.fullfillmentSystem" as "consignment_fullfillment_system",
        $"consignment.fullfillmentType" as "consignment_fullfillment_type",
        $"consignment.fulfillmentSystemRequestId" as "consignment_fulfillmentSystemRequestId",
        $"consignment.fulfillmentSystemResponseId" as "consignment_fulfillmentSystemResponseId",
        $"consignment.invoiceNum" as "invoice_Num",
        $"consignmentEntries" as "consignmentEntries",
        $"consignmentEntries.metadata" as "metadata",
        $"consignment.storeNumber" as "consignment_storeNumber",
        $"consignment.consignmentStatus" as "consignment_status",
        $"consignment.paymentsInfo" as "payments_info",
        $"postedAt",
        $"orderDateTime",
        $"load_time_kafka",
        $"load_time_adls",
        $"presell"
      ).withColumn("giftcard", explode($"metadata")).drop("metadata")
    val snowflake_temp = consignment_snow.withColumn("gift_card_number", regexp_extract($"giftcard","""cardNumber":(\d+)""",0)).withColumn("gift_card_amount", regexp_extract($"giftcard","""transactionAmount":(\d+\.\d+)""",0))
    val snowflake_final = snowflake_temp.withColumn("gift_card_number", $"gift_card_number".substr(lit(13), length($"gift_card_number"))).withColumn("gift_card_amount", $"gift_card_amount".substr(lit(20), length($"gift_card_amount")))
    .withColumn("presell_flag",$"presell").drop("presell")
    .withColumn("payment_transaction_subtype",udfFunc($"payments_info",lit("paymentTransactionSubType"))).drop("giftcard")   
    .withColumn("ref_source", lit("OMS"))
    .withColumn("consignment_fulfillment_center", lit(""))


    writeSnowflakeTable(getSnowflakeConnectionOptions(),
      getTableName("CONSIGNMENTS", inputParams(AppendGen2ToSnowFlakeTablesFlag)),
      snowflake_final,
      SaveMode.Append)


    val consignment_parsed = consignment_explode_1
      .select(
        $"order_id",
//        $"consignmentEntries.requestingSystemLineNo" as "consignment_order_lineNo",
        $"company_number",
        $"orderDateTime",
        $"presell",
        $"consignment.consignmentId" as "consignment_Id",
        $"consignment.fullfillmentSystem" as "consignment_fullfillment_system",
        $"consignment.fullfillmentType" as "consignment_fullfillment_type",
        $"consignment.fulfillmentSystemRequestId" as "consignment_fulfillmentSystemRequestId",
        $"consignment.fulfillmentSystemResponseId" as "consignment_fulfillmentSystemResponseId",
//        $"consignmentEntries.storeNumber" as "fulfilled_storeNumber",
        $"consignment.invoiceNum" as "invoice_Num",
        $"consignmentEntries" as "consignmentEntries",
//        $"consignmentEntries.entryId" as "entryid",
//        $"consignmentEntries.entryStatus" as "status",
//        $"consignmentEntries.cancelCode" as "cancelCode",
//        $"consignmentEntries.cancelReason" as "cancelReason",
//        $"consignmentEntries.sku" as "sku",
//        $"consignmentEntries.size" as "size",
//        $"consignmentEntries.productCode" as "product_code",
//        $"consignmentEntries.productType" as "productType",
       $"consignmentEntries.metadata" as "metadata",
//        $"consignmentEntries.requestedQty" as "requested_qty",
//        $"consignmentEntries.fulfilledQty" as "fulfilledQty",
//        $"consignmentEntries.cancelledQty" as "cancelledQty",
//        $"consignmentEntries.pricing.currencyIso" as "currencyIso",
//        $"consignmentEntries.pricing.priceOverrideReason" as "priceOverrideReason",
//        $"consignmentEntries.pricing.originalRetailPrice" as "original_retail_price",
//        $"consignmentEntries.pricing.unitPrice" as "unit_price",
//        $"consignmentEntries.pricing.subTotalAmount" as "sub_total_amount",
//        $"consignmentEntries.pricing.taxAmount" as "tax_amount",
//        $"consignmentEntries.pricing.shippingAmount" as "shipping_amount",
//        $"consignmentEntries.pricing.shippingTaxAmount" as "shippingTaxAmount",
//        $"consignmentEntries.pricing.giftBoxAmount" as "giftBoxAmount",
//        $"consignmentEntries.pricing.giftBoxTaxAmount" as "giftBoxTaxAmount",
//        $"consignmentEntries.pricing.totalAmount" as "total_amount",
//        $"consignmentEntries.pricing.discountAmount" as "discount_amount",
//        $"consignmentEntries.pricing.discountedTotalAmount" as "discounted_total_amount",
//        $"consignmentEntries.shippedDate" as "shipped_date",
//        $"consignmentEntries.cartonNum" as "carton_num",
//        $"consignmentEntries.cegrRefId" as "cegrRefId", //new
//        $"consignmentEntries.carrier" as "carrier",
//        $"consignmentEntries.trackingId" as "tracking_id",
        $"consignment.storeNumber" as "consignment_storeNumber",
        $"consignment.consignmentStatus" as "consignment_status",
        $"consignment.paymentsInfo" as "payments_info",
//        udfFunc($"payments_info",lit("amount")) as "payment_amount",
//        udfFunc($"payments_info",lit("authCode"))  as "payment_authcode",
//        udfFunc($"payments_info",lit("creditCardType")) as "credit_card_type",
//        udfFunc($"payments_info",lit("date")) as "payment_date",
//        udfFunc($"payments_info",lit("paymentTransactionId")) as "payment_transaction_id",
       udfFunc($"consignment.paymentsInfo",lit("paymentTransactionSubType")) as "payment_transaction_subtype",
      //  udfFunc($"consignment.paymentsInfo",lit("paymentType")) as "payment_type",
//        udfFunc($"payments_info",lit("cegrRefId")) as "payment_cegrRefId",
//        $"consignmentEntries.updated" as "updated_date",
//        $"consignmentEntries.modifiedDate" as "modifiedDate",
        $"postedAt",
        $"load_time_kafka",
        $"load_time_adls"
      ).withColumn("orderdate", to_date($"orderDateTime")).withColumn("giftcard", explode($"metadata")).drop("metadata")

    val consignment_giftcard = consignment_parsed.withColumn("gift_card_number", regexp_extract($"giftcard","""cardNumber":(\d+)""",0)).withColumn("gift_card_amount", regexp_extract($"giftcard","""transactionAmount":(\d+\.\d+)""",0))
    val consignment_final = consignment_giftcard.withColumn("gift_card_number", $"gift_card_number".substr(lit(13), length($"gift_card_number"))).withColumn("gift_card_amount", $"gift_card_amount".substr(lit(20), length($"gift_card_amount"))).drop("giftcard")
    // val consignment_giftcard = consignment_parsed.withColumn("gift_card_exploded",explode($"giftcard")).drop("giftcard")
    // val consignment_final = consignment_giftcard.withColumn("gift_card_number", $"gift_card_exploded.cardNumber").withColumn("gift_card_amount", $"gift_card_exploded.transactionAmount").drop("gift_card_exploded")
    consignment_final.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("orderdate").save(inputParams("pii_landing_path")+ "/sales/oms/consignments/")
    /*########### START WEEKLY VACUUM AND OPTIMIZE ###########*/
    import java.time.LocalDate
    import java.time.DayOfWeek
    import java.time.format.DateTimeFormatter

    val today = LocalDate.now().format(DateTimeFormatter.ISO_DATE)
    val weekDayNumber = LocalDate.now().getDayOfWeek.getValue
    val tablePath = inputParams("pii_landing_path")+ "/sales/oms/consignments/"

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

    //       /*
    //        Filtering out the latest records for refined table
    //        */

    val partitionWindow_consignment = Window.partitionBy($"consignment_Id",$"company_number").orderBy($"postedAt".desc)

    val rownumber_consignment = row_number().over(partitionWindow_consignment)
    val consignment_parsed_filter = consignment_final.select($"*", rownumber_consignment as "rnum")
      .filter($"rnum" === "1")
      .withColumn("update_date",current_timestamp())
      .drop("gift_card_number")
      .drop("gift_card_amount")
      .drop("rnum").withColumn("ref_source", lit("OMS"))
    consignment_parsed_filter.createOrReplaceTempView("consignment_parsed_filter")

    oms_consignment_adls.sparkSession.sql(s"""
          MERGE INTO sales_gen2.oms_consignments t
          USING consignment_parsed_filter s
          ON s.consignment_Id = t.consignment_Id and s.company_number = t.company_number
          WHEN MATCHED THEN UPDATE SET
          t.consignment_fullfillment_system=s.consignment_fullfillment_system,
          t.consignment_fullfillment_type=s.consignment_fullfillment_type,
          t.consignment_fulfillmentSystemRequestId = s.consignment_fulfillmentSystemRequestId,
          t.consignment_fulfillmentSystemResponseId = s.consignment_fulfillmentSystemResponseId,
          t.invoice_Num = s.invoice_Num,
          t.consignmentEntries = s.consignmentEntries,
          t.consignment_storeNumber = s.consignment_storeNumber,
          t.consignment_status = s.consignment_status,
          t.payments_info = s.payments_info,
          t.postedAt=s.postedAt,
          t.load_time_kafka=s.load_time_kafka,
          t.update_date=s.load_time_adls,
          t.presell = s.presell,
          t.payment_transaction_subtype = s.payment_transaction_subtype
           WHEN NOT MATCHED THEN INSERT (order_id, company_number, orderDateTime, presell, consignment_Id, consignment_fullfillment_system, consignment_fullfillment_type, consignment_fulfillmentSystemRequestId, consignment_fulfillmentSystemResponseId, invoice_Num, consignmentEntries, consignment_storeNumber, consignment_status, payments_info, payment_transaction_subtype, postedAt, load_time_kafka, load_time_adls, orderdate, update_date, ref_source)
           VALUES (s.order_id, s.company_number, s.orderDateTime, s.presell, s.consignment_Id, s.consignment_fullfillment_system, s.consignment_fullfillment_type, s.consignment_fulfillmentSystemRequestId, s.consignment_fulfillmentSystemResponseId, s.invoice_Num, s.consignmentEntries, s.consignment_storeNumber, s.consignment_status, s.payments_info, s.payment_transaction_subtype, s.postedAt, s.load_time_kafka, s.load_time_adls, s.orderdate, s.update_date, s.ref_source)
   """)

// ------------------ PowerBi refresh------------------------------------------
  
  val zendesk_url = (inputParams("zendesk_url"))
 val newdataset_url = (inputParams("newdataset_url"))
//val customerService_refresh = "https://api.powerbi.com/v1.0/myorg/groups/7a64bfeb-e6fc-4555-bc9a-3ee380b42b2a/datasets/cce8d0ac-a995-44b7-9767-da952f65c28a/refreshes"
  if(open_orders_flag =="on"){
    println("triggering consignment refresh ")
    val routes = Array(newdataset_url,zendesk_url)
    refreshPowerbi(routes)
    println("refreshing dataset:"+ routes)
  }

    // consignment_explode_1.unpersist()

  }


}