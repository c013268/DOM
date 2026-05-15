package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp_Gen2
import com.footlocker.oms_streaming.apps.AdlsLoadMain_Gen2.sparkSession
import com.footlocker.services.{ADLSService, SnowflakeHelperService_Gen2}
import com.footlocker.services.ADLSService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, prepareDf, readSqlFile}
import com.footlocker.utils.Constants.AppendGen2ToSnowFlakeTablesFlag
import com.footlocker.utils.Logging
import io.delta.tables.DeltaTable
import com.footlocker.utils.AuditUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.footlocker.utils.Common_Gen2.{processSubstitutions, stringToMap}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.sql.Timestamp
import java.time.Instant


object NetworkATP_DOM extends SnowflakeHelperService_Gen2 with Logging{

  def dom_netwotk_atp_load(spark: SparkSession ,network_atp_table: String, network_atp_history_table: String, auditTable: String ,loadType: String, varSubstitutions: String ): Unit = {


    val jobName = "DOM_NETWORK_ATP"

    val targetTable = "network_atp_table"

    val runId = s"${jobName}_${System.currentTimeMillis()}"

    val lastProcessedTs =
      getEffectiveWatermark(spark, auditTable, jobName, loadType)

    println(s"Last processed watermark: $lastProcessedTs")

    insertAuditStart(spark, auditTable, jobName, targetTable, runId, loadType)

    try {

      import spark.implicits._

      val sqlFileName = "sqls/DOM-NETWORK_ATP.sql"
      logInfo(s"Reading Network_atp SQL from resource file: $sqlFileName")
      val rawSql = readSqlFile(sqlFileName)
      val substitutionMap = stringToMap(varSubstitutions)
      val sourceSql = processSubstitutions(rawSql, substitutionMap)

      val statements = sourceSql.split(";").map(_.trim).filter(_.nonEmpty)
      statements.foreach { stmt =>
        spark.sql(stmt)
      }


      val network_atp_source = spark.sql(s"select * from network_atp WHERE load_time_adls > '$lastProcessedTs'")

      // Write all new records to the history table (append-only)
      println("Writing to network_atp_history table")
      network_atp_source
        .write
        .format("delta")
        .mode("append")
        .partitionBy("load_date")
        .saveAsTable(network_atp_history_table)

      // Deduplicate records to get the latest one for each key
      val windowSpec = Window.partitionBy(col("orgId"), col("productId"), col("gtin"), col("sellingChannel"))
        .orderBy(col("updateTime").desc)

      val final_df = network_atp_source
        .withColumn("rank", row_number().over(windowSpec))
        .where(col("rank") === 1)
        .drop("rank")

      println("Merging into network_atp refined table")

      // Perform Merge (Upsert) into the main refined table
      DeltaTable.forName(spark, network_atp_table)
        .as("t")
        .merge(
          final_df.as("s"),
          "t.sellingChannel = s.sellingChannel and t.productId = s.productId and t.orgId = s.orgId and t.gtin = s.gtin and t.messageType = s.messageType"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()


      val newMaxTs: Timestamp = network_atp_source.agg(max("load_time_adls")).head().getTimestamp(0) match { case ts if ts != null => ts; case _ => lastProcessedTs }

      //val rowCount = returns.count()
      println("Network_atp source rowCount for this run: "+network_atp_source.count())


      updateAuditSuccess(spark, auditTable, runId, newMaxTs)

      println("Network_atp load competed")
    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        // Specifically handle missing table/view
        updateAuditFailure(spark, auditTable, runId, s"Source table not found: ${ex.getMessage}")
        logError(s"Load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex // Re-throw the exception to fail the job

      case ex: Exception =>
        // All other failures
        updateAuditFailure(spark, auditTable, runId, ex.getMessage)
        logError(s"Load failed with an unexpected exception: ${ex.getMessage}", ex)
        throw ex

    }
  }
}