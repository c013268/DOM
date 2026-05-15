package com.footlocker.oms_streaming.apps

import com.footlocker.AbstractApp_Gen2
import com.footlocker.utils.Logging
import com.footlocker.conf.AppConfObject_Gen2.setSnowflakeConnectionOptions
import com.footlocker.services.SnowflakeHelperService_Gen2

/**
 * Spark application to run ATP (Available-to-Promise) logic.
 * This can run Network ATP, OBF Location ATP, or both based on input parameters.
 */
object AdlsLoadMain_ATP extends AbstractApp_Gen2 with SnowflakeHelperService_Gen2 with Logging {

  override def execute(inputParams: Map[String, String]): Unit = {

    val spark = sparkSession

    spark.conf.set("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    setSnowflakeConnectionOptions(inputParams)

    val loadType = inputParams("loadType")
    val atpRunOption = inputParams.getOrElse("atp_run_option", "both").toLowerCase
    val appendGen2ToSnowflake = inputParams.getOrElse("AppendGen2ToSnowFlakeTablesFlag", "n")
    val varSubstitutions = inputParams.getOrElse("varSubstitutions", "")

    logInfo(s"Executing AdlsLoadMain_ATP with run option: '$atpRunOption'")

    if (atpRunOption == "network" || atpRunOption == "both") {
      val network_atp_table = inputParams.getOrElse("network_atp_table", throw new IllegalArgumentException("Missing required parameter: network_atp_table for network ATP run"))
      val network_atp_history_table = inputParams.getOrElse("network_atp_history_table", throw new IllegalArgumentException("Missing required parameter: network_atp_history_table for network ATP run"))
      val auditTable = inputParams.getOrElse("auditTable", throw new IllegalArgumentException("Missing required parameter: auditTable for network ATP run"))
      logInfo("Running Network ATP load.")
      NetworkATP_DOM.dom_netwotk_atp_load(spark, network_atp_table, network_atp_history_table, auditTable, loadType, varSubstitutions)
      logInfo("Finished Network ATP load.")
    }

    if (atpRunOption == "obf_location" || atpRunOption == "both") {
      logInfo("Running OBF ATP Location load.")
      LocationAtp_DOM.dom_obf_atp_location_load(spark, inputParams, varSubstitutions)
      logInfo("Finished OBF ATP Location load.")
    }

    if (atpRunOption != "network" && atpRunOption != "obf_location" && atpRunOption != "both") {
      val errorMessage = s"Invalid 'atp_run_option' provided: '$atpRunOption'. Valid options are 'network', 'obf_location', or 'both'."
      logError(errorMessage)
      throw new IllegalArgumentException(errorMessage)
    }
  }
}