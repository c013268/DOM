package com.footlocker.conf

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.services.SnowflakeHelperService_Gen2
import com.footlocker.utils.Constants.SnowflakeDBScope
import com.typesafe.config.{Config, ConfigFactory}

/**
 * The Object stores the generic Configuration data for all the apps
 */
object AppConfObject_Gen2 extends SnowflakeHelperService_Gen2 {
  lazy val _oms_client_id = dbutils.secrets.get(scope = "powerbi-oms-realtime-app-scope", key = "client-id")
  lazy val _oms_client_secret = dbutils.secrets.get(scope = "powerbi-oms-realtime-app-scope", key = "client-secret")

  private var sfUrl = ""
  private var sfUser = ""
  private var sfPassword = ""
  private var sfDatabase = ""
  private var sfSchema = ""
  private var sfWarehouse = ""

  def getSnowflakeConnectionOptions() = {
    Map(
      "sfUrl" -> sfUrl,
      "sfUser" -> sfUser,
      "sfPassword" -> sfPassword,
      "sfDatabase" -> sfDatabase,
      "sfSchema" -> sfSchema,
      "sfWarehouse" -> sfWarehouse)
  }

  def setSnowflakeConnectionOptions(inputParams: Map[String, String]) = {
    println(s"""Getting and setting up dbutil secrets for scope:${inputParams(SnowflakeDBScope)}""")
    sfUrl = dbutils.secrets.get(scope = inputParams(SnowflakeDBScope), key = "snowflake_url")
    sfUser = dbutils.secrets.get(scope = inputParams(SnowflakeDBScope), key = "username")
    sfPassword = dbutils.secrets.get(scope = inputParams(SnowflakeDBScope), key = "password")
    sfDatabase = dbutils.secrets.get(scope = inputParams(SnowflakeDBScope), key = "database") //"DBIMART_PRE_PROD_DOM" //
    sfSchema = "CUSTOMER_REPORTING"
    sfWarehouse = dbutils.secrets.get(scope = inputParams(SnowflakeDBScope), key = "warehouse")
  }

  def PreProdOptions() = {
    println(s"Getting and setting up dbutil secrets for scope:snowflake-dbi-scope")

    val sfUrl = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "snowflake_url")
    val sfUser = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "username")
    val sfPassword = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "password")
    val sfWarehouse = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "warehouse")

    Map(
      "sfURL" -> sfUrl,
      "sfUser" -> sfUser,
      "sfPassword" -> sfPassword,
      "sfDatabase" -> "DBIMART_UAT",
      "sfSchema" -> "CUSTOMER_REPORTING",
      "sfWarehouse" -> sfWarehouse
    )
  }

  def oms_client_id: String = _oms_client_id

  def oms_client_secret: String = _oms_client_secret

}