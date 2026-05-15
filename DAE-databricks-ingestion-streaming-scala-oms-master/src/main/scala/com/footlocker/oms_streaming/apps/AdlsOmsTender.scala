package com.footlocker.oms_streaming.apps

import com.footlocker.services.SnowflakeHelperService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, refreshPowerbi, udfFunc}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, from_json, to_date}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.sql.Date
import scala.io.Source
import com.footlocker.oms_streaming.apps.Kafkareadstream_Gen2.readFromKafkaStream
import org.apache.spark.sql.functions.{col, from_json, to_date}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.spark.SparkContextProvider
import org.apache.spark.sql.SparkSession

object AdlsOmsTender extends SnowflakeHelperService_Gen2 {
  def parsedOrdersFunc(oms_tender_adls: DataFrame, spark: SparkSession, inputParams : Map[String, String]): Unit =  {

    oms_tender_adls.sparkSession.conf.set("spark.sql.shuffle.partitions", "auto")
    oms_tender_adls.sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    oms_tender_adls.sparkSession.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")


    import spark.implicits._
    val messageTypeSchema = StructType(
      List(
        StructField("messageType", StringType, true),
        StructField("orderId", StringType, true),
        StructField("omsOrderId", StringType, true),
        StructField("companyNumber", StringType, true),
        StructField("orderStatus", StringType, true),
        StructField("cancelCode", StringType, true),
        StructField("postedAt", StringType, true),
        StructField("postedBy", StringType, true)
      )
    )
    val oms_message_parsed_data = oms_tender_adls
      .withColumn("messageType1", from_json($"json",messageTypeSchema))
      .withColumn("messageType", $"messageType1.messageType").withColumn("orderId", $"messageType1.orderId").withColumn("omsOrderId", $"messageType1.omsOrderId").withColumn("companyNumber", $"messageType1.companyNumber").withColumn("orderStatus", $"messageType1.orderStatus").drop("messageType1")
    oms_message_parsed_data.createOrReplaceTempView("oms_message_parsed_data")
    println("oms message parsed")

    val parsed = applyschema(spark,oms_message_parsed_data).select("oms_parsed_data.payment.*","oms_parsed_data.messageType","oms_parsed_data.orderId","oms_parsed_data.omsOrderId","oms_parsed_data.companyNumber","oms_parsed_data.orderStatus", "oms_parsed_data.order.orderheader.orderPricing.discountedTotalAmount","load_time_kafka").withColumn("paymentTransactions", coalesce($"paymentTransactions", array()))
    parsed.createOrReplaceTempView("parsed")
    //parsed.repartition(20).write.mode(SaveMode.Append).format("delta").option("mergeSchema", "true").save(inputParams("oms_parsed_path"))
    val inline = oms_tender_adls.sparkSession.sql(s"""select currencyIso,messageType,orderId,omsOrderId,companyNumber,orderStatus,inline(arrays_zip(authorizations,paymentTransactions)), discountedTotalAmount,date(load_time_kafka) as load_date from parsed""")
    inline.createOrReplaceTempView("inline")

    val parseOrdersDf = inline.withColumnRenamed("discountedTotalAmount", "orderHeader_discountedTotalAmount")
        .withColumn("attributes", $"authorizations".getItem("attributes"))
        .withColumn("authorizations_paymentType", $"authorizations".getItem("paymentType"))
        .withColumn("authorizations_authAmount", $"authorizations".getItem("authAmount"))
        .withColumn("authorizations_authCode", $"authorizations".getItem("authCode"))
        .withColumn("authorizations_gateway", $"authorizations".getItem("gateway"))
        .withColumn("authorizations_paymentVendor", $"authorizations".getItem("paymentVendor"))
        .withColumn("authorizations_status", $"authorizations".getItem("status"))
        .withColumn("authorizations_transactionDate", $"authorizations".getItem("transactionDate"))
        .withColumn("authorizations_transactionId", $"authorizations".getItem("transactionId"))
        .withColumn("paymentTransactions_consignmentId", $"paymentTransactions".getItem("consignmentId"))
        .withColumn("paymentTransactions_refundId", $"paymentTransactions".getItem("refundId"))
        .withColumn("paymentTransactions_transaction", $"paymentTransactions".getItem("transaction"))
        .withColumn("attributes_authResponse", $"attributes".getItem("authResponse"))
        .withColumn("attributes_avsCode", $"attributes".getItem("avsCode"))
        .withColumn("attributes_cardAlias", $"attributes".getItem("cardAlias"))
        .withColumn("attributes_cardBin", $"attributes".getItem("cardBin"))
        .withColumn("attributes_cardLast4", $"attributes".getItem("cardLast4"))
        .withColumn("attributes_cardToken", $"attributes".getItem("cardToken"))
        .withColumn("attributes_cardType", $"attributes".getItem("cardType"))
        .withColumn("attributes_cardTypeDisplay", $"attributes".getItem("cardTypeDisplay"))
        .withColumn("attributes_confirmationCode", $"attributes".getItem("confirmationCode"))
        .withColumn("attributes_cvvResponse", $"attributes".getItem("cvvResponse"))
        .withColumn("attributes_expirationDate", $"attributes".getItem("expirationDate"))
        .withColumn("attributes_fundingSource", $"attributes".getItem("fundingSource"))
        .withColumn("attributes_sellerProtectionStatus", $"attributes".getItem("sellerProtectionStatus"))
        .withColumn("paymentTransactions_transaction_amount", $"paymentTransactions_transaction".getItem("amount"))
        .withColumn("paymentTransactions_transaction_authCode", $"paymentTransactions_transaction".getItem("authCode"))
        .withColumn("paymentTransactions_transaction_cardLast4", $"paymentTransactions_transaction".getItem("cardLast4"))
        .withColumn("paymentTransactions_transaction_creditCardType", $"paymentTransactions_transaction".getItem("creditCardType"))
        .withColumn("paymentTransactions_transaction_date", $"paymentTransactions_transaction".getItem("date"))
        .withColumn("paymentTransactions_transaction_id", $"paymentTransactions_transaction".getItem("id").cast("string"))
        .withColumn("paymentTransactions_transaction_paymentTransactionRequestId", $"paymentTransactions_transaction".getItem("paymentTransactionRequestId"))
        .withColumn("paymentTransactions_transaction_paymentTransactionType", $"paymentTransactions_transaction".getItem("paymentTransactionType"))
        .withColumn("paymentTransactions_transaction_paymentType", $"paymentTransactions_transaction".getItem("paymentType"))
        .withColumn("paymentTransactions_transaction_status", $"paymentTransactions_transaction".getItem("status"))
        .withColumn("authorizations_preSettled", $"authorizations".getItem("preSettled"))
        .withColumn("authorizations_originalOrderNumber", $"authorizations".getItem("originalOrderNumber"))
        .withColumn("attributes_storeMerchantId", $"attributes".getItem("storeMerchantId"))
        .withColumn("attributes_storeTerminalId", $"attributes".getItem("storeTerminalId"))
        .withColumn("attributes_storeOrderRequestId", $"attributes".getItem("storeOrderRequestId"))
        .withColumn("attributes_storeInvoiceId", $"attributes".getItem("storeInvoiceId"))
        .withColumn("attributes_giftCardNumber", $"attributes".getItem("giftCardNumber"))
        .withColumn("attributes_email", md5($"attributes".getItem("email")))
        .withColumn("attributes_payerStatus", $"attributes".getItem("payerStatus")).drop("attributes").drop("authorizations").drop("paymentTransactions").drop("paymentTransactions_transaction")
        parseOrdersDf.filter("((authorizations_authAmount is not null and trim(lower(authorizations_authAmount)) not in ('','null')) or (trim(lower(attributes_cardAlias)) is not null and trim(lower(attributes_cardAlias)) not in ('','null')))").repartition(20).write.partitionBy("load_date").mode(SaveMode.Append).option("mergeSchema", "true").format("delta").save(inputParams("oms_tender_parsed_path"))

    /*########### START WEEKLY VACUUM AND OPTIMIZE ###########*/
    import java.time.LocalDate
    import java.time.DayOfWeek
    import java.time.format.DateTimeFormatter

    val today = LocalDate.now().format(DateTimeFormatter.ISO_DATE)
    val weekDayNumber = LocalDate.now().getDayOfWeek.getValue
    val tablePath = inputParams("oms_tender_parsed_path")

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
  }

  def applyschema(spark: SparkSession,df:DataFrame): DataFrame = {
    import spark.implicits._
    val oms_schema = spark.read
      .option("multiLine", true).option("mode", "PERMISSIVE")
      .json("dbfs:/mnt/repo/oms08022023.json").schema
    // APPLYING SCHEMA ON RAW READSTREAM
    df.select(from_json($"json", schema=oms_schema)
      .as("oms_parsed_data"),$"load_time_kafka".alias("load_time_kafka"),$"load_time_adls".alias("load_time_adls"))
  }

}