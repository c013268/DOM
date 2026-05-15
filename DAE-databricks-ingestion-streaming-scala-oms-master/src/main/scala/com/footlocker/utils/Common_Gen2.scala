package com.footlocker.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.conf.AppConfObject_Gen2._
import com.footlocker.utils.Constants.{AppendGen2ToSnowFlakeTablesFlag, NpiiAdlsScope, NpiiAdlsStorageAccount, PiiAdlsScope, PiiAdlsStorageAccount, SnowflakeDBScope}
import com.microsoft.aad.adal4j.{AuthenticationContext, ClientCredential}
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{from_json, lit, udf}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.uaparser.scala.Parser
import org.uaparser.scala.Device
import org.uaparser.scala.OS
import java.util.regex.Pattern


import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object Common_Gen2 {

  // useragent parse logic
  def parseArgs(args: Array[String]): Map[String, String] = {

    val inputParams: Map[String, String] = args(0) match {
      case "azure" => Map(
        "oms_topic" -> args(1),
        "kafka_servers" -> args(2),
        "npii_refined_path" -> args(3),
        "checkpoint_path" -> args(4),
        "pii_landing_path" -> args(5),
        "schema_location" -> args(6),
        "refresh_flag" -> args(7),
        "openOrders_url" -> args(8),
        "zendesk_url" -> args(9),
        "newdataset_url" -> args(10),
        "appName" -> args(11),
        "kafkaScope" -> args(12),
        "kafkaClusterApiKey" -> dbutils.secrets.get(scope = args(12), key = args(13)),
        "kafkaClusterApiSecret" -> dbutils.secrets.get(scope = args(12), key = args(14)),
        NpiiAdlsStorageAccount -> args(15),
        NpiiAdlsScope -> args(16),
        PiiAdlsStorageAccount -> args(17),
        PiiAdlsScope -> args(18),
        AppendGen2ToSnowFlakeTablesFlag -> args(19),
        SnowflakeDBScope -> args(20),
        "oms_parsed_path" -> args(21),
        "oms_tender_parsed_path" -> args(22),
        "table_oms_parsed_path" -> args(23),
        "offset_limit" -> args(24),
        "read_mode" -> args(25),
        "env" -> "azure"
      )

      case "databricks" =>
        Map(
            "delta_initial_path" -> args(1),
            "exchange_landing_table" -> args(2),
            "obf_order_history_table" -> args(3),
            "refund_refined_table" -> args(4),
            "refund_landing_table" -> args(5),
            "return_refined_table" -> args(6),
            "return_landing_table" -> args(7),
            "order_refined_table" -> args(8),
            "order_landing_table" -> args(9),
            "consignments_refined_table" -> args(10),
            "consignments_landing_table" -> args(11),
            "oms_parsed_tender_table" -> args(12),
            "loadType" -> args(13),
            "SnowflakeDBScope" -> args(14),
            "varSubstitutions" -> args(15),
            "tableLoadTypes" -> (if (args.length > 16) args(16) else ""),
            "run_table_group" -> (if (args.length > 17) args(17) else ""),
            "run_table_name" -> (if (args.length > 18) args(18) else "all"),
            "env" -> "azure"
        )

      case "atp" =>
        val runOption = args(1)
        val baseParams = Map("atp_run_option" -> runOption, "env" -> "azure")

        runOption match {
          case "network" => baseParams ++ Map(
            "loadType" -> args(2),
            "network_atp_table" -> args(3),
            "network_atp_history_table" -> args(4),
            "auditTable" -> args(5),
            "varSubstitutions" -> args(6),
            "SnowflakeDBScope" -> args(7)
          )
          case "obf_location" => baseParams ++ Map(
            "loadType" -> args(2),
            "location_atp_target_table" -> args(3),
            "location_atp_history_table" -> args(4),
            "auditTable" -> args(5),
            "varSubstitutions" -> args(6),
            "SnowflakeDBScope" -> args(7)
          )
          case "both" => baseParams ++ Map(
            "loadType" -> args(2),
            "network_atp_table" -> args(3),
            "network_atp_history_table" -> args(4),
            "auditTable" -> args(5),
            "location_atp_target_table" -> args(6),
            "location_atp_history_table" -> args(7),
            "varSubstitutions" -> args(8),
            "SnowflakeDBScope" -> args(9)
          )
          case _ => throw new IllegalArgumentException(s"Invalid 'atp_run_option' provided: '$runOption'. Valid options are 'network', 'obf_location', or 'both'.")
        }

    }
    println(s"Printing inputParams.")
    for ((k,v) <- inputParams) println(s"key: $k, value: $v.")
    inputParams
  }

  def retry[T](n: Int, delay_secs: Int)(fn: => T): T = {
    Try {
      fn
    } match {
      case Success(x) => x
      case _ if n > 1 => {
        println(s"Process failed. Retrying.")
        Thread.sleep(delay_secs * 1000)
        retry(n - 1, delay_secs)(fn)
      }
      case Failure(e) => throw e
    }
  }

  def applyschema(spark: SparkSession, df: DataFrame, schemapath: String): DataFrame = {
    import spark.implicits._
    val oms_schema = spark.read
      .option("multiLine", true).option("mode", "PERMISSIVE")
      .json(schemapath).schema

    // APPLYING SCHEMA ON RAW READSTREAM
    df.select(from_json($"json", schema = oms_schema).as("oms_parsed_data"), $"load_time_kafka".alias("load_time_kafka"), $"load_time_adls".alias("load_time_adls"))

  }

  def isPCCheck(device: Device , uaString: String, os: OS): Boolean = {
    val MOBILE_DEVICE_FAMILIES = Seq("iPhone",
      "iPod",
      "Generic Smartphone",
      "Generic Feature Phone",
      "PlayStation Vita",
      "iOS-Device")
    val TABLET_DEVICE_FAMILIES = Seq("iPad",
      "BlackBerry Playbook",
      "Blackberry Playbook",
      "Kindle",
      "Kindle Fire",
      "Kindle Fire HD",
      "Galaxy Tab",
      "Xoom",
      "Dell Streak")

    val TABLET_DEVICE_BRANDS = Seq("Generic_Android_Tablet")

    val PC_OS_FAMILIES = Seq("Windows", "Mac OS X", "Linux")

    if (MOBILE_DEVICE_FAMILIES.contains(device.family) ||
      TABLET_DEVICE_FAMILIES.contains(device.family) ||
      TABLET_DEVICE_BRANDS.contains(device.brand)) {
      false
    } else if (os.family == "Windows NT" ||
      PC_OS_FAMILIES.contains(os.family) ||
      (os.family == "Windows" )) {
      true
    } else if (os.family == "Mac OS X" && !uaString.contains("Silk")) {
      true
    } else if (os.family == "Chrome OS") {
      true
    } else if (os.family == "Chromecast") {
      false
    } else if (uaString.contains("Linux") && uaString.contains("X11")) {
      true
    } else {
      false
    }
  }
  // Define a function to parse user agent string
  def parseUserAgent(parser: Parser,uaString: String): UserAgentData = {
    try {

      val os = parser.osParser.parse(uaString)

      val userAgent= parser.userAgentParser.parse(uaString)

      val device = parser.deviceParser.parse(uaString)
      val isPc = isPCCheck(device, uaString, os)



      val osVersion = Seq(
        os.major.getOrElse(""),
        os.minor.getOrElse(""),
        os.patch.getOrElse(""),
        os.patchMinor.getOrElse("")
      ).filter(_.nonEmpty).mkString(".")

      val userAgentVersion = Seq(
        userAgent.major.getOrElse(""),
        userAgent.minor.getOrElse(""),
        userAgent.patch.getOrElse("")
      ).filter(_.nonEmpty).mkString(".")

      UserAgentData(

        Map("family" -> userAgent.family, "version" -> s"[${userAgentVersion}]",  "version_string" -> userAgentVersion),
        Map("family" -> os.family, "version" -> osVersion),
        Map("family" -> device.family, "brand" -> device.brand.getOrElse(""), "model" -> device.model.getOrElse("")),

        s"${ (if (isPc) "PC" else device.family)} / ${os.family} ${osVersion} / ${userAgent.family} ${userAgentVersion}"

      )
    } catch {
      case _: Throwable => UserAgentData(Map(), Map(), Map(), "")
    }
  }


  import play.api.libs.json._

  def parseUserAgent_json(parser: Parser,uaString: String): String = {
    if (uaString == null || uaString.isEmpty) {
      return null
    }

    try {
      val userAgentData = parseUserAgent(parser,uaString)
      val jsonString: String = Json.toJson(userAgentData).toString
      return jsonString
    } catch {
      case _: Throwable => ""
    }
  }
  // Define a simplified version of the function for simple user agent info
  def parseUserAgentSimple(parser: Parser,uaString: String): String = {

    if (uaString == null || uaString.isEmpty) {
      return null
    }

    try {


      val os = parser.osParser.parse(uaString)

      val userAgent= parser.userAgentParser.parse(uaString)

      val device = parser.deviceParser.parse(uaString)
      val isPc = isPCCheck(device, uaString, os)
      val osVersion = Seq(
        os.major.getOrElse(""),
        os.minor.getOrElse(""),
        os.patch.getOrElse(""),
        os.patchMinor.getOrElse("")
      ).filter(_.nonEmpty).mkString(".")

      val userAgentVersion = Seq(
        userAgent.major.getOrElse(""),
        userAgent.minor.getOrElse(""),
        userAgent.patch.getOrElse("")
      ).filter(_.nonEmpty).mkString(".")

      s"${ (if (isPc) "PC" else device.family)} / ${os.family} ${osVersion} / ${userAgent.family} ${userAgentVersion}"

    } catch {
      case _: Throwable => ""
    }
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

  def refreshPowerbi(routes:Array[String]): Unit = {

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

  def prepareDf(spark: SparkSession, df: DataFrame, stdCols: Array[String]): DataFrame = {

    val addlColumns = stdCols.filter(x => !df.columns.contains(x))
    val tgtDf = addlColumns.foldLeft(df) { (df, colName) => df.withColumn(colName, lit(null.asInstanceOf[String])) }

    tgtDf.createOrReplaceTempView("DF")

    val formattedTgtCols = stdCols.map(x => s"\t $x AS $x").mkString(",\n")
    val sqlText = s"SELECT \n$formattedTgtCols \nFROM DF"

    spark.sql(sqlText)
  }

  def readSqlFile(fileName: String): String = {
    println("[APP_LOG] - Reading SQL from :" + fileName)

    val sourceSql = scala.io.Source.fromResource(fileName).getLines()
      .map(line => line.replaceAll("\\-\\-.+", "") // replace single line comments --
        .replaceAll("^\\s*$", ""))
      .filter(line => !line.isEmpty())
      .mkString("\n")

    // replace multi line comments
    val multiLineCommentPattern = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL)
    val commentFreeSql = multiLineCommentPattern.matcher(sourceSql).replaceAll("")
    commentFreeSql
  }

  import java.util.regex.Matcher.{ quoteReplacement => qq }
  def processSubstitutions(input: String, vars: Map[String, String]): String = {
    val substPattern = """\$\{(.+?)\}""".r
    substPattern.replaceAllIn(
      input, { m =>
        val ref = m.group(1)
        qq(vars.getOrElse(ref, ref))
      }
    )
  }

  def stringToMap(param: String): Map[String, String] = {
    //val param = extParams.replaceAll("^\"|\"$", "")
    try {
      if (param.nonEmpty) {
        //Negative look back to handle the separator is used as value itself
        param.split("(?<!(=));").map(x => {
          val k = x.split("(?<!(=))=")
          (k(0), k(1))
        }).toList.toMap
      } else Map[String, String]()
    } catch {
      case e: Exception =>
        println(s"[APP_LOG] - Unable to Convert String to Map : " + param + " to Map", e)
        throw e
    }
  }

  def calculateCheckDigit(sku: String): Integer = {
    if (sku == null) return null
    var intEven = 0
    var intOdd = 0

    for (i <- 0 until sku.length) {
      if ((i + 1) % 2 == 0) {
        intEven += sku(i).asDigit
      } else {
        var intTemp = sku(i).asDigit * 2
        if (intTemp > 9) {
          val strTemp = intTemp.toString
          intOdd += strTemp(0).asDigit + strTemp(1).asDigit
        } else {
          intOdd += intTemp
        }
      }
    }

    val sum = intOdd + intEven
    var intTemp = (sum.toDouble / 10).toInt
    intTemp = (intTemp + 1) * 10
    intTemp = (intTemp - intOdd - intEven) % 10
    intTemp
  }

  def transformSkuWithCheckDigit(legacySku: String): String = {
    if (legacySku == null || legacySku.trim.isEmpty) return null

    try {
      val parts = legacySku.split("-")
      if (parts.length == 4 && parts.forall(_.matches("\\d+"))) {
        val banner = parts(0)
        val dept = parts(1)
        val style = parts(2)
        val colorSize = parts(3)
        val color = colorSize.substring(0, 2)
        val size = colorSize.substring(2)

        val checkDigitInput = banner + dept + style + color
        val checkDigit = calculateCheckDigit(checkDigitInput)

        s"$banner-$dept-$style-$checkDigit-$color-$size"
      } else {
        legacySku
      }
    } catch {
      case _: Exception => legacySku
    }
  }

}
