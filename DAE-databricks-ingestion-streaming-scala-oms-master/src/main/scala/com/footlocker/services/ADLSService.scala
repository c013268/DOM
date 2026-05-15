package com.footlocker.services
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.spark.SparkContextProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
trait ADLSService {

  this : SparkContextProvider =>

  /** Function to retrive ADLS conttection details
    *
    * @param credentialScope Scope of obtaining the Secrets
    * @return tuple of client_id, credential, directory_id, adls_url
    */
  def getADLSSecrets(credentialScope: String): (String, String, String,String) =
    ( dbutils.secrets.get(scope = credentialScope, key = "client_id"),
      dbutils.secrets.get(scope = credentialScope, key = "credential"),
      dbutils.secrets.get(scope = credentialScope, key = "directory_id"),
      dbutils.secrets.get(scope = credentialScope, key = "adls_url")

    )

//  var truststore_pass = dbutils.secrets.get(scope = "kafka-scope", key = "truststore-pass")
//  var keystore_pass = dbutils.secrets.get(scope = "kafka-scope", key = "keystore-pass")
//  var key_pass = dbutils.secrets.get(scope = "kafka-scope", key = "key-pass")
  /**
    * This functions sets the necessary ADLS details to SparkSession Config
    * @param clientId Client Id
    * @param credential Credential
    * @param directoryId Directory Id
    */

  def setAdlsConectivity(clientId:String,credential:String,directoryId:String) : Unit ={
    sparkSession.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
    sparkSession.conf.set("dfs.adls.oauth2.client.id", clientId)
    sparkSession.conf.set("dfs.adls.oauth2.credential", credential)
    sparkSession.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/3698556c-48eb-4511-8a0e-5fb6b7ebb01f/oauth2/token".format(directoryId))
  }

  /**The function reads files from the provided path
    *
    * @param path path where the files should be read from
    * @param schema Schema of the files
    * @return dataframe created from the files read
    */
  def readADLSFiles(path: String, schema: StructType = null): DataFrame = {
    if (schema == null) sparkSession.read.parquet(path)
    else sparkSession.read.schema(schema).parquet(path)
  }

  /**
    *
    * @param path Path in which the files have to be written
    * @param layer adls Layer
    * @param database database in ADLS
    * @param table  table in Adls
    * @param df dataframe which is to be written
    * @param numPartitions number of partitions to be written
    * @param saveMode save mode that should be used for write
    * @param partitionColumns columns used to partition
    */

  def writeADLSFiles(path: String, layer: String, database: String,table: String, df: DataFrame, numPartitions: Int = 0,saveMode: SaveMode = SaveMode.Overwrite, partitionColumns: String = null): Unit = {
    val adlsPath =  (if (path.charAt(path.length-1)=='/') path else path+"/") +
      (if (layer =="") "" else layer+"/") +
      (if (database =="") "" else database+"/") +
      table
    if (numPartitions == 0) {
      if (partitionColumns == null) df.write.mode(saveMode).parquet(adlsPath)
      else df.write.mode(saveMode).partitionBy(partitionColumns).parquet(adlsPath)
    } else {
      if (partitionColumns == null) df.coalesce(numPartitions).write.mode(saveMode).parquet(adlsPath)
      else df.coalesce(numPartitions).write.mode(saveMode).partitionBy(partitionColumns).parquet(adlsPath)
    }
  }



}
