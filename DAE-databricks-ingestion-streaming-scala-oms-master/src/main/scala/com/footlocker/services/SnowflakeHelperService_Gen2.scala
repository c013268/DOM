package com.footlocker.services

import org.apache.spark.sql.{DataFrame, SaveMode}

trait SnowflakeHelperService_Gen2 {

  import net.snowflake.spark.snowflake.Utils

  // Helper to run a SQL statement in Snowflake using provided options
  def runSnowflakeMerge(opt: Map[String, String], sql: String): Unit = {
    Utils.runQuery(opt, sql)
  }


  def getTableName(originalTableName: String, appendGen2Flag: String) = {
    originalTableName + (if (appendGen2Flag.toLowerCase() == "y" || appendGen2Flag.toLowerCase() == "yes") "_GEN2" else "")
  }

  def writeSnowflakeTable(opt: Map[String, String], table: String, df: DataFrame, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    println(s"""Writing to Snowflake table:${table}.""")
    df.write.format("snowflake").mode(saveMode).options(opt).option("dbtable", table).save()
  }


}