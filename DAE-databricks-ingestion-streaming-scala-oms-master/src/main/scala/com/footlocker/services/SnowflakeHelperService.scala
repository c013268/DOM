package com.footlocker.services

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

trait SnowflakeHelperService {

  def getSnowflakeServerDetails(scope: String): (String, String) =
    (dbutils.secrets.get(scope = "snowflake-scope", key = "snowflake-d-user"),
      dbutils.secrets.get(scope = "snowflake-scope", key = "snowflake-d-pass"))

  def getConnectionProperties(): Properties = {
    val props = new Properties
    props.setProperty("driver", "net.snowflake.client.jdbc.SnowflakeDriver")
    props
  }

  def getSnowflakeConnectionOptions(url: String, user: String, password: String, database: String, schema: String, warehouse: String) =
    Map("sfUrl" -> url,
      "sfUser" -> user,
      "sfPassword" -> password,
      "sfDatabase" -> database,
      "sfSchema" -> schema,
      "sfWarehouse" -> warehouse)

  def getSnowflakeServerConnectionString(userName: String, password:String,  database:String, schema :String, warehouse :String) : String =
    "jdbc:snowflake://footlocker.east-us-2.azure.snowflakecomputing.com/?user=" + userName + "&password=" + password + "&authenticator=snowflake&db="+ database +"&schema="+ schema +
      "&warehouse="+ warehouse +"&loginTimeout=60"


  def writeSnowflakeTable(opt: Map[String, String], table : String, df: DataFrame,saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    df.write.format("snowflake").mode(saveMode).options(opt).option("dbtable",table).save()
  }

  def snowflakequeryrun(query:String,options:Map[String, String]): Unit = {
    Utils.runQuery(options, query)
  }
}

