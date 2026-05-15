package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp_Gen2
import com.footlocker.oms_streaming.apps.AdlsLoadMain_Gen2.sparkSession
import com.footlocker.services.ADLSService_Gen2
import com.footlocker.utils.Common_Gen2.{applyschema, prepareDf, readSqlFile}
import com.footlocker.utils.AuditUtils.{getEffectiveWatermark, insertAuditStart, updateAuditSuccess, updateAuditFailure}
import com.footlocker.utils.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.footlocker.utils.Common_Gen2.{processSubstitutions, stringToMap}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.Timestamp
import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


object AdlsConsignments_DOM extends Logging {


  /**
   * Extract view name from a CREATE OR REPLACE TEMP VIEW statement.
   */
  private def extractViewName(stmt: String): String = {
    val upper = stmt.toUpperCase.trim
    val marker = "CREATE OR REPLACE TEMP VIEW"
    if (upper.contains(marker)) {
      upper.split(marker)(1).trim.split("\\s+")(0).trim
    } else "UNKNOWN"
  }

  /**
   * Register multiple SQL views in parallel using the given ExecutionContext.
   * Blocks until ALL views are registered.
   */
  private def registerViewsParallel(spark: SparkSession,stmts: Seq[String],tierLabel: String,ec: ExecutionContext): Unit = {
    implicit val iec: ExecutionContext = ec
    val futures = stmts.map { stmt =>
      val viewName = extractViewName(stmt)
      Future {
        val t = System.currentTimeMillis()
        spark.sql(stmt)
        val elapsed = System.currentTimeMillis() - t
        println(s"    [PAR-$tierLabel] $viewName → ${elapsed}ms")
      }
    }
    Await.result(Future.sequence(futures), 5.minutes)
  }

  def dom_consignments_load(spark: SparkSession, consignments_refined_table: String, consignments_landing_table: String, loadType: String, varSubstitutions: String): Unit = {

    val jobName_1 = "DOM_OMS_CONSIGNMENTS_DATABRICKS_LOAD"
    val targetTable_1 = "oms_consignments"
    val auditTable = "prod.etl_stats_npii.dom_oms_etl_audit"

    // Normalize once — getEffectiveWatermark expects UPPER-CASE ("FULL" / "INCREMENTAL")
    val upperLoadType = Option(loadType).map(_.trim.toUpperCase).getOrElse("INCREMENTAL")

    val runId_1 = s"${jobName_1}_${System.currentTimeMillis()}"

    val lastProcessedTs_1 = getEffectiveWatermark(spark, auditTable, jobName_1, upperLoadType)

    println(s"Last processed watermark Databricks: $lastProcessedTs_1")

    // ─── Watermark-driven lookback dates ───────────────────────────────
    // When audit table is truncated (full load intent), lastProcessedTs_1 = 1900-01-01 sentinel
    // → minusDays produces a very old date → no pruning. For incremental, produces a recent date.
    val baseMap = stringToMap(varSubstitutions)
    val sourceLookbackDays = baseMap.get("source_lookback_days")
      .flatMap(v => scala.util.Try(v.trim.toInt).toOption)
      .getOrElse(10000)
    val mergeTargetBufferDays = 100

    val lookbackDate = lastProcessedTs_1.toLocalDateTime.toLocalDate
      .minusDays(sourceLookbackDays)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val mergeTargetDate = lastProcessedTs_1.toLocalDateTime.toLocalDate
      .minusDays(sourceLookbackDays + mergeTargetBufferDays)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val mergeTargetPredicate = s"and t.orderdate >= '$mergeTargetDate'"
    println(s"  [MERGE] mergeTargetDate=$mergeTargetDate (lookbackDate=$lookbackDate - ${mergeTargetBufferDays}d buffer)")
    // ─── End Watermark-driven lookback dates ───────────────────────────

    insertAuditStart(spark, auditTable, jobName_1, targetTable_1, runId_1, upperLoadType)

    var consignmentsRefinedDf: Option[DataFrame] = None
    var executorService: Option[ExecutorService] = None

    try {

      import spark.implicits._

      println("Consignments load processing")

      val sqlFileName = "sqls/DOM-OMS_CONSIGNMENTS.sql"
      val statements = {
        logInfo(s"Reading SQL from resource file: $sqlFileName")
        val rawSql = readSqlFile(sqlFileName)

        println(s"  [SUB] lookback_date=$lookbackDate (source_lookback_days=$sourceLookbackDays, lastProcessedTs=$lastProcessedTs_1)")

        val substitutionMap = baseMap ++ Map(
          "incremental_ts_1" -> lastProcessedTs_1.toString,
          "incremental_ts_2" -> lastProcessedTs_1.toString,
          "lookback_date"    -> lookbackDate
        )

        val sourceSql = processSubstitutions(rawSql, substitutionMap)
        val stmts = sourceSql.split(";").map(_.trim).filter(_.nonEmpty)
        // Log parsed view names for traceability
        stmts.zipWithIndex.foreach { case (s, i) =>
          println(s"  Parsed SQL[$i]: ${extractViewName(s)}")
        }
        stmts
      }

      // -------------------------------------------------------
      // PARALLEL SQL EXECUTION (retained from v2 optimization)
      // -------------------------------------------------------
      val sqlExecutor = Executors.newFixedThreadPool(5)
      val sqlEc: ExecutionContext = ExecutionContext.fromExecutorService(sqlExecutor)

      try {
        val tier0End = Math.min(3, statements.length)
        // TIER 0: Base views — independent, safe to run in parallel
        registerViewsParallel(spark, statements.slice(0, tier0End), "T0", sqlEc)

        // TIER 1: Remaining views — depend on TIER 0, but independent of each other
        if (statements.length > tier0End) {
          registerViewsParallel(spark, statements.slice(tier0End, statements.length), "T1", sqlEc)
        }
      } finally {
        sqlExecutor.shutdown()
      }


      // Prepare landing payload
      val consignments_landing =
        spark.sql(s"select * from consignments_landing_stg")
          .withColumn("consignmentEntries", filter(col("consignmentEntries"), x => x.getField("productCode").isNotNull))
          .filter(size(col("consignmentEntries")) > 0)
          .withColumn("gift_card_number", col("gift_card_number").cast("string"))
          .withColumn("gift_card_amount", col("gift_card_amount").cast("string"))

      // Read from refined view — SQL
      val consignments = spark.sql(s"select * from consignments")

      // -------------------------------------------------------
      // Materialize + cache refined DataFrame.
      // -------------------------------------------------------
      val consignments_refined = {
        val df = consignments.drop("gift_card_number", "gift_card_amount")
          .withColumn("consignmentEntries", filter(col("consignmentEntries"), x => x.getField("productCode").isNotNull))
          .filter(size(col("consignmentEntries")) > 0)
          .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        val cnt = df.count()
        println(s"  consignments_refined cached: $cnt rows")
        df
      }

      consignmentsRefinedDf = Some(consignments_refined)

      // TempView needed here: refinedFuture references "consignments_refined" by name in the MERGE SQL.
      consignments_refined.createOrReplaceTempView("consignments_refined")

      // Pre-compute max watermark timestamp while DataFrame is cached and hot.
      val newMaxTs_1 =
        Option(consignments_refined.agg(max("load_time_adls").cast("timestamp")).as[Timestamp].first()).getOrElse(lastProcessedTs_1)

      executorService = Some(Executors.newFixedThreadPool(2))
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService.get)


      // --- Landing path (runs in parallel with Delta merge) ---
      consignments_landing.createOrReplaceTempView("consignments_landing_merge")
      val landingFuture = Future {
		spark.sql(
        s"""
			MERGE INTO $consignments_landing_table t
			USING consignments_landing_merge s
			ON s.order_id = t.order_id
				AND s.consignment_Id = t.consignment_Id
				AND s.company_number = t.company_number
				AND s.consignment_status = t.consignment_status
				AND t.ref_source = 'MAO'
				$mergeTargetPredicate
			WHEN MATCHED THEN UPDATE SET
				t.orderDateTime = s.orderDateTime,
				t.consignment_fullfillment_system = s.consignment_fullfillment_system,
				t.consignment_fullfillment_type = s.consignment_fullfillment_type,
				t.consignment_fulfillmentSystemRequestId = s.consignment_fulfillmentSystemRequestId,
				t.consignment_fulfillmentSystemResponseId = s.consignment_fulfillmentSystemResponseId,
				t.invoice_Num = s.invoice_Num,
				t.consignmentEntries = s.consignmentEntries,
				t.consignment_storeNumber = s.consignment_storeNumber,
				t.payments_info = s.payments_info,
				t.postedAt = s.postedAt,
				t.load_time_kafka = s.load_time_kafka,
				t.load_time_adls = s.load_time_adls,
				t.gift_card_number = s.gift_card_number,
				t.gift_card_amount = s.gift_card_amount,
				t.presell = s.presell,
				t.payment_transaction_subtype = s.payment_transaction_subtype,
				t.ref_source = s.ref_source,
				t.orderdate = s.orderdate
			WHEN NOT MATCHED THEN INSERT *
		""")
      }

      // --- Delta merge path (runs in parallel with Landing) ---
      val refinedFuture = Future {
        spark.sql(
          s"""
            MERGE INTO $consignments_refined_table t
            USING consignments_refined s
            ON s.order_id = t.order_id
				and s.consignment_Id = t.consignment_Id
				and s.company_number = t.company_number
				and t.ref_source = 'MAO'
				$mergeTargetPredicate
            WHEN MATCHED THEN UPDATE SET
				t.orderDateTime = s.orderDateTime,
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
				t.load_time_adls = s.load_time_adls,
				t.presell = s.presell,
				t.payment_transaction_subtype = s.payment_transaction_subtype,
				t.ref_source = s.ref_source,
				t.orderdate = s.orderdate
            WHEN NOT MATCHED THEN INSERT (order_id, company_number, orderDateTime, presell, consignment_Id, consignment_fullfillment_system, consignment_fullfillment_type, consignment_fulfillmentSystemRequestId, consignment_fulfillmentSystemResponseId, invoice_Num, consignmentEntries, consignment_storeNumber, consignment_status, payments_info, payment_transaction_subtype, postedAt, load_time_kafka, load_time_adls, orderdate, update_date, ref_source)
            VALUES (s.order_id, s.company_number, s.orderDateTime, s.presell, s.consignment_Id, s.consignment_fullfillment_system, s.consignment_fullfillment_type, s.consignment_fulfillmentSystemRequestId, s.consignment_fulfillmentSystemResponseId, s.invoice_Num, s.consignmentEntries, s.consignment_storeNumber, s.consignment_status, s.payments_info, s.payment_transaction_subtype, s.postedAt, s.load_time_kafka, s.load_time_adls, s.orderdate, s.update_date, s.ref_source)
       """)
      }

      // Wait for all parallel paths to complete
      val landingResult = scala.util.Try(Await.result(landingFuture, 4.hours))
      val deltaResult   = scala.util.Try(Await.result(refinedFuture, 4.hours))

      // Propagate first failure — the outer catch block will handle audit updates
      (landingResult, deltaResult) match {
        case (scala.util.Failure(ex), _) => throw ex
        case (_, scala.util.Failure(ex)) => throw ex
        case _ => // all succeeded
      }

      // Uncache dim_location if it was registered as a temp view (may be a CTE in some SQL versions)
      try { spark.catalog.uncacheTable("dim_location") } catch { case _: Exception => /* not cached */ }

      updateAuditSuccess(spark, auditTable, runId_1, newMaxTs_1)

      println("consignments load competed")
    } catch {

      case ex: org.apache.spark.sql.AnalysisException =>
        updateAuditFailure(spark, auditTable, runId_1, s"Source table not found: ${ex.getMessage}")
        logError(s"Consignments load failed due to AnalysisException: ${ex.getMessage}", ex)
        throw ex

      case ex: Exception =>
        updateAuditFailure(spark, auditTable, runId_1, ex.getMessage)
        logError(s"Consignments load failed with a general exception: ${ex.getMessage}", ex)
        throw ex

    } finally {
      consignmentsRefinedDf.foreach(_.unpersist())
      executorService.foreach(_.shutdown())
    }
  }
}
