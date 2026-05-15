package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp
import com.footlocker.oms_streaming.apps.Kafkareadstream.readFromKafkaStream
import com.footlocker.services.DeltaoptimizeService
import com.footlocker.utils.Common._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, from_json, to_date}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Base class for implementing Spark code. Manages the SparkSession at beginning and end of code
 */
object AdlsLoadMain extends AbstractApp with DeltaoptimizeService {


  override def execute(inputParams: Map[String, String]): Unit = {

    val spark = sparkSession

    spark.conf.set("spark.streaming.concurrentJobs", "15")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    setDatabricksEnv(spark)
    import spark.implicits._
    spark.sparkContext.getConf.setAppName(inputParams("appName"))

    val adls_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/landing/oms/raw_string"""
    val read_mode = inputParams("read_mode")

    //    val raw_landing_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/landing/oms/raw_string"""
    //    val order_refined_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/refined/oms/orders"""
    //    val consignment_refined_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/refined/oms/consignment"""
    //    val return_refined_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/refined/oms/return"""
    //    val exchange_refined_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/refined/oms/exchange"""
    //    val refund_refined_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/refined/oms/refund"""
    //    val parsed_landing_checkpoint = s"""${inputParams("checkpoint_path")}/checkpoint/landing/oms/parsed_landing"""
//    val products = spark.sql("""select trim(product_number) as p_product_number, case When average_cost = 0 Then control_cost
//        else average_cost  end cogs from product.online_us_product""")

    val products = spark.read.load("dbfs:/mnt/product/products_fixed_cost")

    val rawstream = readFromKafkaStream(spark,inputParams)

    def adlsFunc(rawstream: DataFrame, batchId: Long, spark: SparkSession, inputParams : Map[String, String]): Unit = {

      optimize(spark, "OMSStreaming")
      val messageTypeSchema = StructType(
        List(
          StructField("messageType", StringType, true)
        )
      )
      val raw_landing_path = s"""${inputParams("adls_url")}/landing/sales/oms/raw_string_landing"""
      rawstream.cache()
      ///////// Writing Raw String Stream //////////////
      val rawstream_final = rawstream.withColumn("load_date",to_date(col("load_time_adls")))
      rawstream_final.write.format("delta").partitionBy("load_date").mode("append").save(raw_landing_path)

      ///////// Parsing message type //////////////
      val oms_message_parsed = rawstream
        .withColumn("messageType", from_json($"json",messageTypeSchema))
        .withColumn("messageType", $"messageType.messageType")
      oms_message_parsed.createOrReplaceTempView("oms_message_parsed")

      val order_df = oms_message_parsed.filter("messageType=='ORDER'")
      val consignment_df = oms_message_parsed.filter("messageType=='CONSIGNMENT'")
      val refund_df = oms_message_parsed.filter("messageType=='REFUND'")
      val return_df = oms_message_parsed.filter("messageType=='RETURN'")
      val exchange_df = oms_message_parsed.filter("messageType=='EXCHANGE'")
      val chargeback_df = oms_message_parsed.filter("messageType=='CHARGEBACK'")

      if(order_df.count() > 0){
        AdlsOrders.AdlsOrdersFunc(order_df,spark, inputParams, products)
      }

      if(consignment_df.count() > 0){
        AdlsConsignment.consignment_adls(consignment_df,spark, inputParams)
      }

      if(refund_df.count() > 0){
        AdlsRefund.refund_adls_push(refund_df,spark, inputParams)
      }

      if(return_df.count() > 0){
        AdlsReturn.AdlsReturnFunc(return_df,spark, inputParams)
      }

      if(exchange_df.count() > 0){
        AdlsExchange.exchange_adls_push(exchange_df,spark, inputParams)
      }

      if(chargeback_df.count() > 0){
        AdlsChargeback.AdlsChargebackFunc(chargeback_df,spark, inputParams)
      }

      rawstream.unpersist()
    }
    if (read_mode == "stream") {
      rawstream.writeStream
        .option("checkpointLocation", adls_checkpoint)
        .foreachBatch((rawstream: DataFrame, batchId: Long) => adlsFunc(rawstream, batchId, spark, inputParams))
        .trigger(Trigger.ProcessingTime("60 seconds"))
        .start()
        .awaitTermination()
    }
    else{
      rawstream.writeStream
        .option("checkpointLocation", adls_checkpoint)
        .foreachBatch((rawstream: DataFrame, batchId: Long) => adlsFunc(rawstream, batchId, spark, inputParams))
        .trigger(Trigger.Once())
        .start()
        .awaitTermination()
    }

  }
}