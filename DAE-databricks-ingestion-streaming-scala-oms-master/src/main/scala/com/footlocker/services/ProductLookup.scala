package com.footlocker.services

import org.apache.spark.sql.SparkSession

trait ProductLookup {

  //  @transient var _broadcastVariable: org.apache.spark.broadcast.Broadcast[scala.collection.immutable.Map[String, String]] = setBroadCastVariable;
  var lookupMap: org.apache.spark.broadcast.Broadcast[scala.collection.immutable.Map[String, String]] = null;

  def update_status(spark: SparkSession,job:String,variable_no:Int) = {
    this.synchronized {

      println(s"variable has been recreated and going to update the flag to L for table online_us_product and $job and $variable_no ")


      try {
        spark.sql(s"""update etl_stats.tbl_load_status set load_status = 'L' where table = 'online_us_product' and trim(job) = '$job' and variable_no = $variable_no""")
              }
              catch{
                case e:Exception => {
                  Thread.sleep(10000)
                  spark.sql(s"""update etl_stats.tbl_load_status set load_status = 'L' where table = 'online_us_product' and trim(job) = '$job' and variable_no = $variable_no""")
                }
              }

      println(s"variable has been recreated for table online_us_product and $job and $variable_no to L -loaded")
    }
  }



  def setLookupValue(spark: SparkSession) = {

    val product = spark.sql("select cast(trim(product_number) as String),trim(average_cost) from product.online_us_product")
    val map = product
      .collect()
      .map(e => (e.getString(0), e.getString(1)))
      .toMap
    lookupMap = spark.sparkContext.broadcast(map)
  }



  def getlookupMap(spark: SparkSession, job: String) = {
    val loadStatus =
      spark.sql(s"select cast(upper(load_status) as String) from etl_stats.tbl_load_status where table = 'online_us_product' and trim(job) = '$job' and variable_no = 1").repartition(1)
        .collect()(0)(0)
        .toString()
    if (loadStatus == "S" || lookupMap == null) {
      if (lookupMap != null) {
        lookupMap.destroy()
      }
      setLookupValue(spark: SparkSession)
      println("variable destroyed loaded")

      update_status(spark,job,1)

    }
    lookupMap

  }







  var lookupMap2: org.apache.spark.broadcast.Broadcast[scala.collection.immutable.Map[String, String]] = null;

  def setLookupValue2(spark: SparkSession) = {

    val product2 = spark.sql("select cast(trim(product_number) as String),trim(average_cost) from product.online_us_product")
    val map2 = product2
      .collect()
      .map(e => (e.getString(0), e.getString(1)))
      .toMap
    lookupMap2 = spark.sparkContext.broadcast(map2)
  }

  def getlookupMap2(spark: SparkSession, job: String) = {
    val loadStatus2 =
      spark.sql(s"select cast(upper(load_status) as String) from etl_stats.tbl_load_status where table = 'online_us_product' and trim(job) = '$job' and variable_no = 2").repartition(1)
        .collect()(0)(0)
        .toString()
    if (loadStatus2 == "S" || lookupMap2 == null) {
      if (lookupMap2 != null) {
        lookupMap2.destroy()
      }
      setLookupValue2(spark: SparkSession)
      println("variable destroyed loaded")

      update_status(spark,job,2)

    }
    lookupMap2

  }



}





//println("variable destroyed loaded")
//setLookupValue2(spark: SparkSession)
//println("variable2 has been recreated and going to update the flag in adls to L -loaded")
//spark.sql(s"""update etl_stats.tbl_load_status set load_status = 'L' where table = 'online_us_product' and trim(job) = '$job' and variable_no = 2""")

//      println("variable destroyed loaded")
//      setLookupValue(spark: SparkSession)
//      println("variable has been recreated and going to update the flag in adls to L -loaded")
//      try {
//        spark.sql(s"""update etl_stats.tbl_load_status set load_status = 'L' where table = 'online_us_product' and trim(job) = '$job' and variable_no = 1""")
//      }
//      catch{
//        case e:Exception => {
//          Thread.sleep(20000)
//          spark.sql(s"""update etl_stats.tbl_load_status set load_status = 'L' where table = 'online_us_product' and trim(job) = '$job' and variable_no = 1""")
//        }
//      }