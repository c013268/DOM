package com.footlocker.oms_streaming.apps

import com.footlocker.conf.AppConfObject_Gen2.getSnowflakeConnectionOptions
import com.footlocker.services.SnowflakeHelperService_Gen2
import com.footlocker.utils.Common_Gen2.applyschema
import com.footlocker.utils.Constants.AppendGen2ToSnowFlakeTablesFlag
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object AdlsChargeback_Gen2 extends SnowflakeHelperService_Gen2 {

  def AdlsChargebackFunc(oms_chargeback_adls: DataFrame, spark: SparkSession, inputParams: Map[String, String]) {
    oms_chargeback_adls.sparkSession.conf.set("spark.sql.shuffle.partitions", 2)
    oms_chargeback_adls.sparkSession.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    //READING SCHEMA FILE
    import spark.implicits._
    //FILTERING OUT CHARGEBACK MESSAGE
    val schemapath:String = (inputParams("schema_location")+"/chargeback.json")
    val parsed = applyschema(spark, oms_chargeback_adls, schemapath)

    val chargeback_explode = parsed.select(parsed("oms_parsed_data.OrderId").alias("OrderId"),
      parsed("oms_parsed_data.chargebacks").alias("chargebacks"),
      parsed("oms_parsed_data.payment").alias("payment"),
      parsed("oms_parsed_data.postedBy").alias("postedBy"),
      parsed("oms_parsed_data.postedAt").alias("postedAt"),
      parsed("load_time_kafka").alias("load_time_kafka"),
      parsed("load_time_adls").alias("load_time_adls")).withColumn("chargeback_history",explode_outer($"chargebacks.chargebackHistory"))

    val chargeback_latest = chargeback_explode
      .select($"OrderId",
        $"chargeback_history.activeChargeBack" as "activeChargeBack",
        $"chargeback_history.amount" as "amount",
        $"chargeback_history.cegrRefId" as "cegrRefId",
        $"chargeback_history.chargebackDescription" as "chargebackDescription",
        $"chargebacks.totalDisputedAmount" as "totalDisputedAmount",
        $"chargeback_history.date" as "chargebacktimestamp",
        to_date($"chargeback_history.date") as "chargebackdate",
        $"payment",
        $"postedBy",
        $"postedAt",
        $"load_time_kafka",
        $"load_time_adls")
    //chargeback_latest.persist()

    chargeback_latest.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("chargebackdate").save(inputParams("pii_landing_path")+ "/sales/oms/chargeback")

    val chargeback_refined = chargeback_latest.drop("payment").withColumn("updated_datetime",col("load_time_adls")).withColumn("ref_source", lit("OMS"))

    writeSnowflakeTable(getSnowflakeConnectionOptions,
      getTableName("CHARGEBACK", inputParams(AppendGen2ToSnowFlakeTablesFlag)),
      chargeback_refined,
      SaveMode.Append)


    val partitionWindow = Window.partitionBy($"OrderId",$"cegrRefId").orderBy($"chargebacktimestamp".desc)
    val rownumber = row_number().over(partitionWindow)


    val dedup_chargeback_refined = chargeback_refined.select($"*", rownumber as "rnum")
      .filter($"rnum" === "1")
      .drop("rnum").withColumn("ref_source", lit("OMS"))
    dedup_chargeback_refined.createOrReplaceTempView("chargeback_refined")


    oms_chargeback_adls.sparkSession.sql(s"""
          MERGE INTO sales_gen2.oms_chargebacks t
          USING chargeback_refined s
          ON s.OrderId = t.OrderId AND s.cegrRefId = t.cegrRefId
          WHEN MATCHED THEN UPDATE SET
          t.activeChargeBack = s.activeChargeBack ,
          t.amount = s.amount ,
          t.cegrRefId = s.cegrRefId,
          t.chargebackDescription = s.chargebackDescription,
          t.totalDisputedAmount = s.totalDisputedAmount ,
          t.chargebacktimestamp = s.chargebacktimestamp ,
          t.chargebackdate = s.chargebackdate ,
          t.postedBy = s.postedBy ,
          t.postedAt = s.postedAt ,
          t.load_time_kafka = s.load_time_kafka ,
          t.updated_datetime = s.load_time_adls
          WHEN NOT MATCHED THEN INSERT (OrderId, activeChargeBack, amount, cegrRefId, chargebackDescription, totalDisputedAmount, chargebacktimestamp, chargebackdate, postedBy, postedAt, load_time_kafka, load_time_adls, updated_datetime, ref_source)
          VALUES (s.OrderId, s.activeChargeBack, s.amount, s.cegrRefId, s.chargebackDescription, s.totalDisputedAmount, s.chargebacktimestamp, s.chargebackdate, s.postedBy, s.postedAt, s.load_time_kafka, s.load_time_adls, s.updated_datetime, s.ref_source)
        """)

    //oms_chargeback_adls.unpersist()


  }
}