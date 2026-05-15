package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp_Gen2
import com.footlocker.utils.Common_Gen2._
import com.footlocker.utils.Logging
import com.footlocker.oms_streaming.apps._
import com.footlocker.conf.AppConfObject_Gen2.setSnowflakeConnectionOptions
import com.footlocker.services.SnowflakeHelperService_Gen2
import play.api.libs.json.Json
import java.time.LocalDate
import java.time.DayOfWeek
import java.time.format.DateTimeFormatter

/**
 * A generic Spark application to run SQL queries from a file for testing purposes.
 * This version uses hardcoded value for the SQL file and substitution variables,
 * making it easy to run for initial tests without passing arguments.
 */
object AdlsLoadMain_DOM extends AbstractApp_Gen2 with SnowflakeHelperService_Gen2 with Logging {

  /*########### WEEKLY VACUUM AND OPTIMIZE HELPER ###########*/
  private def weeklyVacuumAndOptimize(spark: org.apache.spark.sql.SparkSession, tableName: String): Unit = {
    val today = LocalDate.now().format(DateTimeFormatter.ISO_DATE)
    val weekDayNumber = LocalDate.now().getDayOfWeek.getValue

    println(s"Checking history to run weekly vacuum and optimize for [$tableName] on DATE $today, WEEKDAY $weekDayNumber")

    if (weekDayNumber == 7) { // Sunday
      val historyDf = spark.sql(s"DESCRIBE HISTORY $tableName")

      val optimizeToday = historyDf.filter("operation = 'OPTIMIZE'").filter(
        historyDf("timestamp").cast("date") === today
      ).count() == 0

      val vacuumToday = historyDf.filter("operation = 'VACUUM END'").filter(
        historyDf("timestamp").cast("date") === today
      ).count() == 0

      if (optimizeToday) {
        println(s"Running OPTIMIZE on: $tableName")
        spark.sql(s"OPTIMIZE $tableName")
      } else {
        println(s"Skipping OPTIMIZE for $tableName as it already ran today")
      }

      if (vacuumToday) {
        println(s"Running VACUUM on: $tableName with retention 168 hours")
        spark.sql(s"VACUUM $tableName RETAIN 168 HOURS")
      } else {
        println(s"Skipping VACUUM for $tableName as it already ran today")
      }
    } else {
      println(s"Skipping VACUUM & OPTIMIZE for [$tableName] as it is not Sunday")
    }
  }
  /*########### WEEKLY VACUUM AND OPTIMIZE HELPER END ###########*/

  override def execute(inputParams: Map[String, String]): Unit = {
    val spark = sparkSession

    // Delta write optimizations
    spark.conf.set("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    // Adaptive Query Execution — dynamically optimizes joins, coalesces small partitions
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "auto")

    // Broadcast small dimension tables (dim_location, lkp_cancel_code, dim_mao_loc) — 50MB threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")

    spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")

    setSnowflakeConnectionOptions(inputParams)

    val exchange_landing_table =inputParams("exchange_landing_table")
    val obf_order_history_table =inputParams("obf_order_history_table")
    val refund_refined_table = inputParams("refund_refined_table")
    val refund_landing_table = inputParams("refund_landing_table")
    val return_refined_table = inputParams("return_refined_table")
    val return_landing_table = inputParams("return_landing_table")
    val order_refined_table = inputParams("order_refined_table")
    val order_landing_table = inputParams("order_landing_table")
    val consignments_refined_table = inputParams("consignments_refined_table")
    val consignments_landing_table = inputParams("consignments_landing_table")
    val oms_parsed_tender_table = inputParams("oms_parsed_tender_table")
    val loadType = inputParams("loadType")                          //loadType= "INCREMENTAL"  (global default)
    val varSubstitutions = inputParams("varSubstitutions")
    val run_table_group = inputParams.get("run_table_group").filter(_.trim.nonEmpty) //  
    val run_table_name = inputParams.getOrElse("run_table_name", "all") match {
      case s if s.trim.isEmpty => "all"
      case s => s
      }
    val tableLoadTypesJson = inputParams.getOrElse("tableLoadTypes", "") //tableLoadTypes  = {"orders": "INCREMENTAL", "returns": "FULL"}

    val tableLoadTypes: Map[String, String] =
      if (tableLoadTypesJson.nonEmpty)
        Json.parse(tableLoadTypesJson).asOpt[Map[String, String]].getOrElse(Map.empty[String, String])
      else Map.empty[String, String]

    val tablesToRun: Seq[String] = run_table_group
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq(run_table_name))

    def effectiveLoadType(table: String): String =
      tableLoadTypes.getOrElse(table, loadType)

    def runTable(table: String): Unit = {
      val tableLoadType = effectiveLoadType(table)
      println(s"Running table=[$table] with loadType=[$tableLoadType]")

      table match {
        case "orders" =>
          AdlsOrders_DOM.dom_orders_load(spark, order_refined_table, order_landing_table, tableLoadType, varSubstitutions)
          weeklyVacuumAndOptimize(spark, order_refined_table)
          weeklyVacuumAndOptimize(spark, order_landing_table)
        case "consignments" =>
          AdlsConsignments_DOM.dom_consignments_load(spark, consignments_refined_table, consignments_landing_table, tableLoadType, varSubstitutions)
          weeklyVacuumAndOptimize(spark, consignments_refined_table)
          weeklyVacuumAndOptimize(spark, consignments_landing_table)
        case "refunds" =>
          AdlsRefund_DOM.dom_refund_load(spark, refund_refined_table, refund_landing_table, tableLoadType, varSubstitutions)
          weeklyVacuumAndOptimize(spark, refund_refined_table)
          weeklyVacuumAndOptimize(spark, refund_landing_table)
        case "returns" =>
          AdlsReturn_DOM.dom_return_load(spark, return_refined_table, return_landing_table, tableLoadType, varSubstitutions)
          weeklyVacuumAndOptimize(spark, return_refined_table)
          weeklyVacuumAndOptimize(spark, return_landing_table)
        case "exchanges" =>
          AdlsExchange_DOM.dom_exchanges_load(spark, exchange_landing_table, tableLoadType, varSubstitutions)
          weeklyVacuumAndOptimize(spark, exchange_landing_table)
        case "obf_order_history" =>
          OBF_Order_Status_History_DOM.dom_obf_order_history_load(spark, obf_order_history_table, tableLoadType, varSubstitutions)
          weeklyVacuumAndOptimize(spark, obf_order_history_table)
        case "tenders" =>
          AdlsOmsTender_DOM.dom_tenders_load(spark, oms_parsed_tender_table, tableLoadType, varSubstitutions)
          weeklyVacuumAndOptimize(spark, oms_parsed_tender_table)
        case "all" =>
          AdlsOrders_DOM.dom_orders_load(spark, order_refined_table, order_landing_table, effectiveLoadType("orders"), varSubstitutions)
          weeklyVacuumAndOptimize(spark, order_refined_table)
          weeklyVacuumAndOptimize(spark, order_landing_table)
          AdlsConsignments_DOM.dom_consignments_load(spark, consignments_refined_table, consignments_landing_table, effectiveLoadType("consignments"), varSubstitutions)
          weeklyVacuumAndOptimize(spark, consignments_refined_table)
          weeklyVacuumAndOptimize(spark, consignments_landing_table)
          AdlsRefund_DOM.dom_refund_load(spark, refund_refined_table, refund_landing_table, effectiveLoadType("refunds"), varSubstitutions)
          weeklyVacuumAndOptimize(spark, refund_refined_table)
          weeklyVacuumAndOptimize(spark, refund_landing_table)
          AdlsReturn_DOM.dom_return_load(spark, return_refined_table, return_landing_table, effectiveLoadType("returns"), varSubstitutions)
          weeklyVacuumAndOptimize(spark, return_refined_table)
          weeklyVacuumAndOptimize(spark, return_landing_table)
          AdlsExchange_DOM.dom_exchanges_load(spark, exchange_landing_table, effectiveLoadType("exchanges"), varSubstitutions)
          weeklyVacuumAndOptimize(spark, exchange_landing_table)
          OBF_Order_Status_History_DOM.dom_obf_order_history_load(spark, obf_order_history_table, effectiveLoadType("obf_order_history"), varSubstitutions)
          weeklyVacuumAndOptimize(spark, obf_order_history_table)
          AdlsOmsTender_DOM.dom_tenders_load(spark, oms_parsed_tender_table, effectiveLoadType("tenders"), varSubstitutions)
          weeklyVacuumAndOptimize(spark, oms_parsed_tender_table)
        case _ =>
          throw new IllegalArgumentException(s"Unknown table: $table")
      }
    }

    tablesToRun.foreach(runTable)
  }
}
