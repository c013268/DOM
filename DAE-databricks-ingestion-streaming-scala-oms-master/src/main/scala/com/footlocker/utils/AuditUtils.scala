package com.footlocker.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.Instant

object AuditUtils {

  // -----------------------------------------------------------------
  // Schema defined once – avoids repeated inference and mergeSchema
  // drift when writing audit rows.
  // -----------------------------------------------------------------
  private val auditSchema = StructType(Seq(
    StructField("job_name",          StringType,    nullable = true),
    StructField("target_table",      StringType,    nullable = true),
    StructField("last_processed_ts", TimestampType, nullable = true),
    StructField("status",            StringType,    nullable = true),
    StructField("error_message",     StringType,    nullable = true),
    StructField("run_id",            StringType,    nullable = true),
    StructField("load_type",         StringType,    nullable = true),
    StructField("started_at",        TimestampType, nullable = true),
    StructField("completed_at",      TimestampType, nullable = true)
  ))

  // -----------------------------------------------------------------
  // Watermark helpers (unchanged behaviour)
  // -----------------------------------------------------------------
  def getLastWatermark(spark: SparkSession, auditTable: String, jobName: String): Timestamp = {
    import spark.implicits._
    spark.table(auditTable)
      .filter(col("job_name") === lit(jobName) && col("status") === "success")
      .select(max("last_processed_ts"))
      .collect()
      .headOption
      .flatMap(row => Option(row.getTimestamp(0)))
      .getOrElse(Timestamp.valueOf("1900-01-01 00:00:00"))
  }

  def getEffectiveWatermark(spark: SparkSession, auditTable: String, jobName: String, loadType: String): Timestamp =
    loadType match {
      case "FULL"        => Timestamp.valueOf("1900-01-01 00:00:00")
      case "INCREMENTAL" => getLastWatermark(spark, auditTable, jobName)
    }

  // -----------------------------------------------------------------
  // SINGLE audit-start insert (original single-job callers)
  // -----------------------------------------------------------------
  def insertAuditStart(spark: SparkSession, auditTable: String, jobName: String,
                       targetTable: String, runId: String, loadType: String): Unit =
    insertAuditStartBatch(spark, auditTable, Seq((jobName, targetTable, runId, loadType)))

  // -----------------------------------------------------------------
  // BATCH audit-start insert – writes BOTH run rows in ONE Delta commit
  // instead of two separate append transactions.
  //
  // entries: Seq of (jobName, targetTable, runId, loadType)
  // -----------------------------------------------------------------
  def insertAuditStartBatch(spark: SparkSession, auditTable: String,
                             entries: Seq[(String, String, String, String)]): Unit = {
    val now = Timestamp.from(Instant.now())
    val rows = entries.map { case (jobName, targetTable, runId, loadType) =>
      Row(jobName, targetTable, null, "running", null, runId, loadType, now, null)
    }
    spark.createDataFrame(java.util.Arrays.asList(rows: _*), auditSchema)
      .write
      .mode(SaveMode.Append)
      .saveAsTable(auditTable)
  }

  // -----------------------------------------------------------------
  // SINGLE audit-success update (kept for backward compatibility)
  // -----------------------------------------------------------------
  def updateAuditSuccess(spark: SparkSession, auditTable: String,
                         runId: String, newMaxTs: Timestamp): Unit =
    spark.sql(
      s"""UPDATE $auditTable
         |SET    status            = 'success',
         |       last_processed_ts = TIMESTAMP('$newMaxTs'),
         |       completed_at      = current_timestamp()
         |WHERE  run_id = '$runId'
         |AND    status = 'running'""".stripMargin)

  // -----------------------------------------------------------------
  // BATCH audit-success update – updates BOTH run IDs in ONE Delta
  // transaction (single write lock, single Delta log commit).
  //
  // Usage: updateAuditSuccessBatch(spark, table,
  //          Seq(runId_1 -> ts1, runId_2 -> ts2))
  // -----------------------------------------------------------------
  def updateAuditSuccessBatch(spark: SparkSession, auditTable: String,
                               updates: Seq[(String, Timestamp)]): Unit = {
    val caseExpr = updates
      .map { case (runId, ts) => s"WHEN run_id = '$runId' THEN TIMESTAMP('$ts')" }
      .mkString("\n      ")
    val runIds = updates.map { case (runId, _) => s"'$runId'" }.mkString(", ")
    spark.sql(
      s"""UPDATE $auditTable
         |SET    status            = 'success',
         |       last_processed_ts = CASE $caseExpr END,
         |       completed_at      = current_timestamp()
         |WHERE  run_id IN ($runIds)
         |AND    status = 'running'""".stripMargin)
  }

  // -----------------------------------------------------------------
  // SINGLE audit-failure update (kept for backward compatibility)
  // -----------------------------------------------------------------
  def updateAuditFailure(spark: SparkSession, auditTable: String,
                         runId: String, errorMsg: String): Unit = {
    val safeError = errorMsg.replace("'", " ")
    spark.sql(
      s"""UPDATE $auditTable
         |SET    status        = 'failed',
         |       error_message = '$safeError',
         |       completed_at  = current_timestamp()
         |WHERE  run_id = '$runId'
         |AND    status = 'running'""".stripMargin)
  }

  // -----------------------------------------------------------------
  // BATCH audit-failure update – marks BOTH run IDs failed in ONE
  // Delta transaction with the same error message.
  // -----------------------------------------------------------------
  def updateAuditFailureBatch(spark: SparkSession, auditTable: String,
                               runIds: Seq[String], errorMsg: String): Unit = {
    val safeError = errorMsg.replace("'", " ")
    val runIdList = runIds.map(r => s"'$r'").mkString(", ")
    spark.sql(
      s"""UPDATE $auditTable
         |SET    status        = 'failed',
         |       error_message = '$safeError',
         |       completed_at  = current_timestamp()
         |WHERE  run_id IN ($runIdList)
         |AND    status = 'running'""".stripMargin)
  }
}
