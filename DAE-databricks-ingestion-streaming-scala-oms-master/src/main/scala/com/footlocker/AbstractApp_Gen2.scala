
package com.footlocker

import com.footlocker.spark.{SparkContextProvider, SparkRegistrator}
import com.footlocker.utils.Common_Gen2.parseArgs


/**
  * Base class for implementing Spark code. Manages the SparkSession at beginning and end of code
  */
abstract class AbstractApp_Gen2 extends SparkContextProvider {

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