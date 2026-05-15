//package com.footlocker
//
//import com.footlocker.services.ADLSService
//import com.footlocker.spark.{SparkContextProvider, SparkRegistrator}
//import com.footlocker.utils.WinUtilsLoader
//import com.footlocker.conf.AppConfObject._
//
///**
//  * Base class for implementing Spark code. Manages the SparkSession at beginning and end of code
//  */
//abstract class AbstractApp extends SparkContextProvider with ADLSService{
//
//  /**
//    * Main entry point for the application, it starts the SparkSession before execute()
//    *
//    * @param args program arguments
//    */
//  def main(args: Array[String]): Unit = {
//    WinUtilsLoader.loadWinUtils()
//    createSparkSession(args(0), this.getClass.toString, classOf[SparkRegistrator])
//    val adlsSecrets = getADLSSecrets(ADLSScope)
//    setAdlsConectivity(adlsSecrets._1,adlsSecrets._2,adlsSecrets._3)
//    execute(args)
//  }
//
//  def execute(args: Array[String])
//}


package com.footlocker

import com.footlocker.spark.{SparkContextProvider, SparkRegistrator}
import com.footlocker.utils.Common.parseArgs


/**
  * Base class for implementing Spark code. Manages the SparkSession at beginning and end of code
  */
abstract class AbstractApp extends SparkContextProvider {

  /**
    * Main entry point for the application, it starts the SparkSession before execute() and stops it after execute()
    *
    * @param args program arguments
    */
  def main(args: Array[String]): Unit = {


    val inputParams = parseArgs(args)

    createSparkSession(inputParams, this.getClass.toString, classOf[SparkRegistrator])
    execute(inputParams)
  }

  /**
    * Override this to add functionality to your application
    */
  def execute(inputParams: Map[String,String])
}