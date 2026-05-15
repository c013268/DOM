package com.footlocker.services

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.spark.SparkContextProvider
import com.footlocker.utils.Constants.{NpiiAdlsScope, NpiiAdlsStorageAccount, PiiAdlsScope, PiiAdlsStorageAccount}
import org.apache.spark.sql.SparkSession

trait ADLSService_Gen2 {

  this: SparkContextProvider =>

  def setUpGen2Env(spark: SparkSession, inputParams: Map[String, String]): Unit = {
    val npiiAdlsSecrets = getADLSGen2Secrets(inputParams(NpiiAdlsScope))
    setAdlsGen2Conectivity(spark, npiiAdlsSecrets._1, npiiAdlsSecrets._2, inputParams(NpiiAdlsStorageAccount))

    val piiAdlsSecrets = getADLSGen2Secrets(inputParams(PiiAdlsScope))
    setAdlsGen2Conectivity(spark, piiAdlsSecrets._1, piiAdlsSecrets._2, inputParams(PiiAdlsStorageAccount))

  }

  private def getADLSGen2Secrets(credentialScope: String): (String, String) = {
    (dbutils.secrets.get(scope = credentialScope, key = "client_id"),
      dbutils.secrets.get(scope = credentialScope, key = "client_secret"))
  }

  private def setAdlsGen2Conectivity(spark: SparkSession, client_id: String, client_secret: String, adlsGen2Name: String): Unit = {
    spark.conf.set(s"fs.azure.account.auth.type.$adlsGen2Name.dfs.core.windows.net", "OAuth")
    spark.conf.set(s"fs.azure.account.oauth.provider.type.$adlsGen2Name.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(s"fs.azure.account.oauth2.client.id.$adlsGen2Name.dfs.core.windows.net", client_id)
    spark.conf.set(s"fs.azure.account.oauth2.client.secret.$adlsGen2Name.dfs.core.windows.net", client_secret)
    spark.conf.set(s"fs.azure.account.oauth2.client.endpoint.$adlsGen2Name.dfs.core.windows.net", "https://login.microsoftonline.com/3698556c-48eb-4511-8a0e-5fb6b7ebb01f/oauth2/token")
  }


}
