package com.footlocker.conf

import com.typesafe.config.{Config, ConfigFactory}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.services.SnowflakeHelperService

/**
 * The Object stores the generic Configuration data for all the apps
 */
  object AppConfObject extends SnowflakeHelperService {
    lazy val config: Config = ConfigFactory.load().getConfig("conf")
    lazy val _ADLSScope : String = config.getString("ADLSScope")
    lazy val _ADLSDatabase : String   =  config.getString("ADLSDatabase")
    lazy val _ADLSLayer : String   =  config.getString("ADLSLayer")
    lazy val _ADLSPath : String = dbutils.secrets.get(scope = ADLSScope, key = "adls_url")
    lazy val  _oms_client_id = dbutils.secrets.get(scope = "powerbi-oms-realtime-app-scope", key = "client-id")
    lazy val  _oms_client_secret = dbutils.secrets.get(scope = "powerbi-oms-realtime-app-scope", key = "client-secret")


    lazy val _SFUrl: String = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "snowflake_url")
    lazy val _SFDatabase : String = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "database")
    //lazy val _SFSchema : String = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "schema")
    lazy val _SFWarehouse : String = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "warehouse")
    lazy val _SFUsername : String = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "username")
    lazy val _SFPassword : String = dbutils.secrets.get(scope = "snowflake-dbi-scope", key = "password")

    def ADLSScope : String = _ADLSScope
    def ADLSPath : String = _ADLSPath
    def ADLSDatabase : String = _ADLSDatabase
    def ADLSLayer : String = _ADLSLayer
    def oms_client_id : String = _oms_client_id
    def oms_client_secret : String = _oms_client_secret

    def SFUrl : String = _SFUrl
    def SFDatabase : String = _SFDatabase
    val SFSchema : String = "CUSTOMER_REPORTING"
    def SFWarehouse : String = _SFWarehouse
    def SFUsername : String = _SFUsername
    def SFPassword : String = _SFPassword

    lazy val _Options : Map[String, String] = getSnowflakeConnectionOptions (SFUrl,SFUsername,
      SFPassword,
      SFDatabase,
      SFSchema,
      SFWarehouse)

    def Options = _Options

  }

