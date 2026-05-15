package com.footlocker.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.conf.AppConfObject._
import com.microsoft.aad.adal4j.{AuthenticationContext, ClientCredential}
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{from_json, udf,lit}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object Common{

    def parseArgs(args: Array[String]): Map[String,String] = {

      val inputParams: Map[String, String] = args(0) match{
        case "azure" => Map(
          "oms_topic" -> args(1),
          "kafka_servers" -> args(2),
          "npii_refined_path" -> args(3),
          "checkpoint_path" -> args(4),
          "adls_url" -> args(5),
          "schema_location" -> args(6),
          "refresh_flag" -> args(7),
          "openOrders_url" -> args(8),
          "zendesk_url" -> args(9),
          "newdataset_url" -> args(10),
          "appName" -> args(11),
          "kafkaScope" -> args(12),
          "kafkaClusterApiKey" -> dbutils.secrets.get(scope = args(12), key = args(13)),
          "kafkaClusterApiSecret" -> dbutils.secrets.get(scope = args(12), key = args(14)),
          "offset_limit" -> args(15),
          "read_mode" -> args(16),
          "env" -> "azure"
        )
        case "local" => Map(
          "delta_initial_path" -> args(1),
          "env" -> "azure"

      )
    }
    println(s"Printing inputParams.")
    for ((k,v) <- inputParams) println(s"key: $k, value: $v.")
    inputParams
  }


    def setDatabricksEnv(spark: SparkSession): Unit = {

      val client_id = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "client_id")
      val credential = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "credential")
      val directory_id = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "directory_id")
      val adls_url = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "adls_url")


      spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
      spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
      spark.conf.set("dfs.adls.oauth2.client.id", client_id)
      spark.conf.set("dfs.adls.oauth2.credential", credential)
      spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/3698556c-48eb-4511-8a0e-5fb6b7ebb01f/oauth2/token".format(directory_id))

    }


    def retry[T](n: Int, delay_secs: Int)(fn: => T): T = {
      Try { fn } match {
        case Success(x) => x
        case _ if n > 1 => {
          println(s"Process failed. Retrying.")
          Thread.sleep(delay_secs*1000)
          retry(n - 1, delay_secs)(fn)
        }
        case Failure(e) => throw e
      }
    }

  def applyschema(spark: SparkSession,df:DataFrame,schemapath:String): DataFrame = {
    import spark.implicits._
    val oms_schema = spark.read
      .option("multiLine", true).option("mode", "PERMISSIVE")
      .json(schemapath).schema

    // APPLYING SCHEMA ON RAW READSTREAM
    df.select(from_json($"json", schema=oms_schema).as("oms_parsed_data"),$"load_time_kafka".alias("load_time_kafka"),$"load_time_adls".alias("load_time_adls"))

  }


  def nofstreamsrunning(spark:SparkSession): Int =
  {
  var count = 0
    val listquery : Array[StreamingQuery] = spark.streams.active

    for(x<-listquery){
      println("Status of id ",x.id,x.status)
      count = count + 1
    }

    count
  }

  def arrayparse(input:mutable.WrappedArray[GenericRowWithSchema],element:String):List[String]={
      input match
      {
      case null=>null
      case input:mutable.WrappedArray[GenericRowWithSchema]=>{
      var lst=List[String]()
      for(x<-input){
      println("type",x.getAs[String](element))
      lst=lst:+x.getAs[String](element)
      }
      lst
      }
}
}
  lazy val _udfFunc= udf(arrayparse _)

  def udfFunc  = _udfFunc

  def refreshPowerbi(routes:Array[String]){

  val authority_url = "https://login.windows.net/footlocker.com/"
  var service: java.util.concurrent.ExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1)
  var context: AuthenticationContext = new AuthenticationContext(authority_url, true, service)
  var result = context.acquireToken("https://analysis.windows.net/powerbi/api", new ClientCredential(oms_client_id, oms_client_secret), null)
  var authToken = result.get().getAccessToken
  val httpClient =  HttpClientBuilder.create().build()
  try{
  for (route <- routes) {
    val configlist = route.split(" ")

    if (pbiRefreashCondition(configlist(1),configlist(2))) {

      val httpPost = new HttpPost(configlist(0))
      httpPost.setHeader("Accept", "application/json")
      httpPost.setHeader("Content-Type", "application/json")
      httpPost.setHeader("Authorization", "Bearer " + authToken);

      val response = httpClient.execute(httpPost)

      if (response.getStatusLine.getStatusCode == 400) {
        println("refresh in progress--skipping")
      }

      else if (response.getStatusLine.getStatusCode == 202) {
        println("request accepted refresh triggered")
        Thread.sleep(2000)
      }
      else {
        print("####################response is " + response.getStatusLine.getStatusCode)
      }

    }
    else{
      println("Refresh condition not met skipping refresh "+route)
    }
  }
  }
    catch{  
          
          case ex: Throwable =>println("found a unknown exception"+ ex)  
      }  
  httpClient.close()
  service.shutdown()
  
  }


  def pbiRefreashCondition(minhr:String,maxhr:String): Boolean = {
    val dT = Calendar.getInstance()
    val formatter = new SimpleDateFormat("HH")
    formatter.setTimeZone(TimeZone.getTimeZone("America/New_York"))

    val esthour = formatter.format(dT.getTime).toInt

    if( minhr.toInt < esthour && esthour < maxhr.toInt ) return true else false

  }

  def prepareDf(spark: SparkSession,df: DataFrame, stdCols: Array[String]): DataFrame = {

    val addlColumns = stdCols.filter(x => !df.columns.contains(x))
    val tgtDf = addlColumns.foldLeft(df) { (df, colName) => df.withColumn(colName, lit(null.asInstanceOf[String])) }

    tgtDf.createOrReplaceTempView("DF")

    val formattedTgtCols = stdCols.map(x => s"\t $x AS $x").mkString(",\n")
    val sqlText = s"SELECT \n$formattedTgtCols \nFROM DF"

    spark.sql(sqlText)
  }




}