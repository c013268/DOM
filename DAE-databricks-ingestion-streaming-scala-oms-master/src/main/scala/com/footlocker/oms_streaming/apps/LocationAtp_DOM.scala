package com.footlocker.oms_streaming.apps

import com.footlocker.conf.AppConfObject_Gen2.getSnowflakeConnectionOptions
import com.footlocker.services.SnowflakeHelperService_Gen2
import com.footlocker.utils.AuditUtils.{_}
import com.footlocker.utils.Common_Gen2.readSqlFile
import com.footlocker.utils.Logging
import io.delta.tables.DeltaTable
import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.footlocker.utils.Common_Gen2.{processSubstitutions, stringToMap}

import java.sql.Timestamp

object LocationAtp_DOM extends SnowflakeHelperService_Gen2 with Logging {

  def dom_obf_atp_location_load(spark: SparkSession, inputParams: Map[String, String], varSubstitutions: String): Unit = {
    val loadType = inputParams("loadType")
    val appendGen2ToSnowflake = inputParams.getOrElse("AppendGen2ToSnowFlakeTablesFlag", "n")
    val location_atp_target_table = inputParams.getOrElse("location_atp_target_table", throw new IllegalArgumentException("Missing required parameter: location_atp_target_table"))
    val location_atp_history_table = inputParams.getOrElse("location_atp_history_table", throw new IllegalArgumentException("Missing required parameter: location_atp_history_table"))
    val auditTable = inputParams.getOrElse("auditTable", throw new IllegalArgumentException("Missing required parameter: auditTable"))

    val jobName = "DOM_LOCATION_ATP"
    val runId = s"${jobName}_${System.currentTimeMillis()}"
    import spark.implicits._

    logInfo("Starting OBF ATP Location DOM load.")

    // This assumes a view or table 'obf_atp_location_feed' is created from the source data.
    // This is analogous to the parsed data from Kafka in the old system.
    val lastProcessedTs = getEffectiveWatermark(spark, auditTable, jobName, loadType)
    logInfo(s"Last processed watermark: $lastProcessedTs")

    insertAuditStart(spark, auditTable, jobName, location_atp_target_table, runId, loadType)

    try {
      val sqlFileName = "sqls/DOM-LOCATION_ATP.sql"
      logInfo(s"Reading Location TP SQL from resource file: $sqlFileName")
      val rawSql = readSqlFile(sqlFileName)
      val substitutionMap = stringToMap(varSubstitutions)
      val sourceSql = processSubstitutions(rawSql, substitutionMap)

      val statements = sourceSql.split(";").map(_.trim).filter(_.nonEmpty)
      statements.foreach { stmt =>
        spark.sql(stmt)
      }

      // Filter for incremental records based on watermark
      val sourceDF = spark.sql(s"SELECT * FROM obf_atp_final WHERE load_time_adls > '$lastProcessedTs'")

      // Deduplicate records to get the latest one for each key, matching the legacy logic.
      val windowSpec = Window.partitionBy("selling_channel", "product_id", "org_id", "fulfillment_type", "location_id", "location_type")
        .orderBy(col("update_time").desc)

      val incrementalDF = sourceDF
        .withColumn("rank", row_number().over(windowSpec))
        .where(col("rank") === 1)
        .drop("rank")
        .withColumn("transaction_type", lit("CHECKOUT")) // Set default transaction_type as in the old process
        .cache()

      val recordCount = incrementalDF.count()
      logInfo(s"Found $recordCount new, unique records to process for source 'MAO'.")

      if (recordCount > 0) {
        // 1. Write all incremental records to the history table
        appendToHistoryTable(incrementalDF, location_atp_history_table)

        // 2. Merge latest incremental records into the main refined table
        mergeIntoDeltaTable(incrementalDF, location_atp_target_table)

        // 3. Write latest incremental records to Snowflake (stage-and-merge)
        writeToSnowflake(incrementalDF, appendGen2ToSnowflake)
      } else {
        logInfo("No new 'MAO' records to process.")
      }

      // Use the source DataFrame before deduplication to get the true max timestamp for the next watermark.
      val newMaxTs: Timestamp = sourceDF.agg(max("load_time_adls")).head().get(0) match {
        case ts: Timestamp => ts
        case _ => lastProcessedTs // Fallback to old watermark if no new records
      }
      updateAuditSuccess(spark, auditTable, runId, newMaxTs)
      incrementalDF.unpersist()

      logInfo("Finished OBF ATP Location DOM load.")
    } catch {
      case ex: Exception =>
        updateAuditFailure(spark, auditTable, runId, ex.getMessage)
        logError(s"OBF ATP Location DOM load failed: ${ex.getMessage}", ex)
        throw ex
    }
  }

  private def mergeIntoDeltaTable(df: DataFrame, targetTableName: String): Unit = {
    logInfo(s"Merging into Delta table: $targetTableName")

    val targetTable = DeltaTable.forName(df.sparkSession, targetTableName)

    targetTable.as("t")
      .merge(
        df.as("s"),
        "t.selling_channel = s.selling_channel AND t.product_id = s.product_id AND t.org_id = s.org_id AND t.fulfillment_type = s.fulfillment_type AND t.location_id = s.location_id AND t.location_type = s.location_type"
      )
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()

    logInfo("Merge into Delta table complete.")
  }

  private def appendToHistoryTable(df: DataFrame, historyTableName: String): Unit = {
    logInfo(s"Appending to history table: $historyTableName")
    df.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable(historyTableName)
    logInfo("Append to history table complete.")
  }

  private def writeToSnowflake(df: DataFrame, appendGen2ToSnowflake: String): Unit = {
    logInfo("Starting Snowflake write process.")

    val (stagetable, finaltable) = if (appendGen2ToSnowflake.toLowerCase == "y") {
      ("STAGE_OBF_ATP_LOCATION_GEN2", s"OBF_ATP_LOCATION_GEN2")
    } else {
      ("STAGE_OBF_ATP_LOCATION", s"OBF_ATP_LOCATION")
    }

    logInfo(s"Using Snowflake tables: Stage -> $stagetable, Final -> $finaltable")

    // Truncate and load stage table
    logInfo(s"Truncating and loading stage table $stagetable.")
    Utils.runQuery(getSnowflakeConnectionOptions(), s"TRUNCATE TABLE $stagetable")
    df.write
      .format("net.snowflake.spark.snowflake")
      .options(getSnowflakeConnectionOptions())
      .option("dbtable", stagetable)
      .mode(SaveMode.Append)
      .save()
    logInfo("Stage table loaded.")

    // Merge from stage to final table
    val mergeSql = s"""
      MERGE INTO $finaltable A USING $stagetable B
      ON A.SELLING_CHANNEL = B.SELLING_CHANNEL
      AND A.PRODUCT_ID = B.PRODUCT_ID
      AND A.ORG_ID = B.ORG_ID
      AND A.FULFILLMENT_TYPE = B.FULFILLMENT_TYPE
      AND A.LOCATION_ID = B.LOCATION_ID
      AND A.LOCATION_TYPE = B.LOCATION_TYPE
      WHEN MATCHED THEN UPDATE SET
        A.TRANSACTION_TYPE = B.TRANSACTION_TYPE, A.GTIN = B.GTIN, A.LAUNCH_DATE = B.LAUNCH_DATE,
        LAUNCH_DATE_TIME = B.LAUNCH_DATE_TIME, UOM = B.UOM, ATP = B.ATP, ATP_STATUS = B.ATP_STATUS,
        DEMAND = B.DEMAND, SAFETY_STOCK = B.SAFETY_STOCK, SEGMENT = B.SEGMENT, SUPPLY = B.SUPPLY,
        FUTURE_QTY_BY_DATES = B.FUTURE_QTY_BY_DATES, UPDATE_TIME = B.UPDATE_TIME,
        LOAD_TIME_KAFKA = B.LOAD_TIME_KAFKA, A.LOAD_TIME_ADLS = B.LOAD_TIME_ADLS, A.LOAD_DATE = B.LOAD_DATE
      WHEN NOT MATCHED THEN INSERT
        (LOCATION_ID, LOCATION_TYPE, ORG_ID, SELLING_CHANNEL, TRANSACTION_TYPE, GTIN, LAUNCH_DATE, LAUNCH_DATE_TIME,
         PRODUCT_ID, UOM, FULFILLMENT_TYPE, ATP, ATP_STATUS, DEMAND, SAFETY_STOCK, SEGMENT, SUPPLY, FUTURE_QTY_BY_DATES,
         UPDATE_TIME, LOAD_TIME_KAFKA, LOAD_TIME_ADLS, LOAD_DATE)
      VALUES
        (B.LOCATION_ID, B.LOCATION_TYPE, B.ORG_ID, B.SELLING_CHANNEL, B.TRANSACTION_TYPE, B.GTIN, B.LAUNCH_DATE, B.LAUNCH_DATE_TIME,
         B.PRODUCT_ID, B.UOM, B.FULFILLMENT_TYPE, B.ATP, B.ATP_STATUS, B.DEMAND, B.SAFETY_STOCK, B.SEGMENT, B.SUPPLY,
         B.FUTURE_QTY_BY_DATES, B.UPDATE_TIME, B.LOAD_TIME_KAFKA, B.LOAD_TIME_ADLS, B.LOAD_DATE)
      """.stripMargin

    logInfo("Executing merge into final Snowflake table.")
    Utils.runQuery(getSnowflakeConnectionOptions(), mergeSql)
    logInfo("Snowflake merge complete.")
  }
}