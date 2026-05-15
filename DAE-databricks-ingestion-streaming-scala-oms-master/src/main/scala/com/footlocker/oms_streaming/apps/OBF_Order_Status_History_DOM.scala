package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp_Gen2
import com.footlocker.oms_streaming.apps.AdlsLoadMain_Gen2.sparkSession
import com.footlocker.services.ADLSService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, prepareDf, readSqlFile}
import com.footlocker.utils.Common_Gen2.{calculateCheckDigit, processSubstitutions, stringToMap, transformSkuWithCheckDigit}
import com.footlocker.utils.Logging
import com.footlocker.utils.AuditUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.Timestamp
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


object OBF_Order_Status_History_DOM extends Logging{


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

  def dom_obf_order_history_load(spark: SparkSession, obf_order_history_table: String, loadType: String, varSubstitutions: String): Unit =  {

    val jobName = "DOM_OBF_ORDER_STATUS_HISTORY_LOAD"
    val targetTable = "OBF_ORDER_STATUS_HISTORY"
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

    val mergeTargetPredicate = s"and t.orderDate >= '$mergeTargetDate'"
    println(s"  [MERGE] mergeTargetDate=$mergeTargetDate (lookbackDate=$lookbackDate - ${mergeTargetBufferDays}d buffer)")
    // ─── End Watermark-driven lookback dates ───────────────────────────

    insertAuditStart(spark, auditTable, jobName, targetTable, runId, upperLoadType)

    var obfFinalDf: Option[DataFrame] = None

    try {

      import spark.implicits._

      println("obf_order_status_history load processing")
      val sqlFileName = "sqls/DOM-OBF_ORDER_STATUS_HISTORY.sql"

      spark.udf.register("transform_sku_with_check_digit", udf((sku: String) => transformSkuWithCheckDigit(sku)))
      spark.udf.register("calculateCheckDigit", udf((sku: String) => {
        if (sku == null) null else calculateCheckDigit(sku)
      }))

      val statements = {
        logInfo(s"Reading OBF SQL from resource file: $sqlFileName")
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

      // Cache & materialize obf_final DF eagerly
      val obf_final = {
        val df = spark.sql(s"select * from obf_final")
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        val cnt = df.count()
        println(s"  obf_final cached: $cnt rows")
        df
      }
      obfFinalDf = Some(obf_final)
      obf_final.createOrReplaceTempView("obf_final")

      // Pre-compute watermark while DF is cached
      val newMaxTs: Timestamp =
        Option(obf_final.agg(max("UPDATEDDATETIME").cast("timestamp")).as[Timestamp].first()).getOrElse(lastProcessedTs)

      // Delta merge — single write path
      spark.sql(
        s"""
    WITH obf_dedup AS (
      SELECT * FROM (
        SELECT
          s.*,
          ROW_NUMBER() OVER (
            PARTITION BY
              orderNumber,
              consignmentId,
              fulfillmentOrderNumber,
              fulfillmentOrderLineNumber,
              status
            ORDER BY updatedDateTime DESC
          ) rn
        FROM obf_final s
      )
      WHERE rn = 1
    )
    MERGE INTO $obf_order_history_table t
    USING obf_dedup s
    ON s.orderNumber  = t.orderNumber
      AND s.consignmentId = t.consignmentId
      AND s.fulfillmentOrderNumber = t.fulfillmentOrderNumber
      AND s.fulfillmentOrderLineNumber = t.fulfillmentOrderLineNumber
      AND s.status = t.status
      AND t.ref_source = 'MAO'
      $mergeTargetPredicate
    WHEN MATCHED THEN UPDATE SET
      t.fulfillmentType            = s.fulfillmentType,
      t.statusCode                 = s.statusCode,
      t.location                   = s.location,
      t.locationType               = s.locationType,
      t.newLocation                = s.newLocation,
      t.orderedQuantity            = s.orderedQuantity,
      t.pickedQuantity             = s.pickedQuantity,
      t.packedQuantity             = s.packedQuantity,
      t.shippedQuantity            = s.shippedQuantity,
      t.currentShippedQuantity     = s.currentShippedQuantity,
      t.cancelledQuantity          = s.cancelledQuantity,
      t.cancelReasonCode           = s.cancelReasonCode,
      t.cancelledBy                = s.cancelledBy,
      t.createdDateTime            = s.createdDateTime,
      t.updatedDateTime            = s.updatedDateTime,
      t.containerNumber            = s.containerNumber,
      t.trackingNumber             = s.trackingNumber,
      t.quantity                   = s.quantity,
      t.shipDate                   = s.shipDate,
      t.carrier                    = s.carrier,
      t.backOrdered                = s.backOrdered,
      t.cpId                       = s.cpId,
      t.originalOrderQuantity      = s.originalOrderQuantity,
      t.organizationCode           = s.organizationCode,
      t.unitPrice                  = s.unitPrice,
      t.expectedDeliveryDate       = s.expectedDeliveryDate,
      t.orderDate                  = s.orderDate,
      t.presell                    = s.presell,
      t.load_date                  = s.load_date,
      t.ref_source                 = s.ref_source
    WHEN NOT MATCHED THEN INSERT (
      consignmentId, orderNumber, fulfillmentType, fulfillmentOrderNumber,
      fulfillmentOrderLineNumber, statusCode, status, location, locationType,
      newLocation, orderedQuantity, pickedQuantity, packedQuantity,
      shippedQuantity, currentShippedQuantity, cancelledQuantity,
      cancelReasonCode, cancelledBy, createdDateTime, updatedDateTime,
      containerNumber, trackingNumber, quantity, shipDate, carrier,
      backOrdered, cpId, originalOrderQuantity, organizationCode,
      unitPrice, expectedDeliveryDate, orderDate, presell, load_date, ref_source
    ) VALUES (
      s.consignmentId, s.orderNumber, s.fulfillmentType, s.fulfillmentOrderNumber,
      s.fulfillmentOrderLineNumber, s.statusCode, s.status, s.location, s.locationType,
      s.newLocation, s.orderedQuantity, s.pickedQuantity, s.packedQuantity,
      s.shippedQuantity, s.currentShippedQuantity, s.cancelledQuantity,
      s.cancelReasonCode, s.cancelledBy, s.createdDateTime, s.updatedDateTime,
      s.containerNumber, s.trackingNumber, s.quantity, s.shipDate, s.carrier,
      s.backOrdered, s.cpId, s.originalOrderQuantity, s.organizationCode,
      s.unitPrice, s.expectedDeliveryDate, s.orderDate, s.presell, s.load_date, s.ref_source
    )
  """)

      updateAuditSuccess(spark, auditTable, runId, newMaxTs)

      println("obf_order_status_history load competed")
    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        updateAuditFailure(spark, auditTable, runId, s"Source table not found: ${ex.getMessage}")
        logError(s"OBF Order Status History load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex

      case ex: Exception =>
        updateAuditFailure(spark, auditTable, runId, ex.getMessage)
        logError(s"OBF Order Status History load failed with a general exception: ${ex.getMessage}", ex)
        throw ex

    } finally {
      obfFinalDf.foreach(_.unpersist())
    }
  }
}
