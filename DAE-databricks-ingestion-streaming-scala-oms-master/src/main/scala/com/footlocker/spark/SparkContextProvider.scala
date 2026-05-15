package com.footlocker.spark

import com.footlocker.utils.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/**
 * Trait that provides the management of a SparkSession
 */
trait SparkContextProvider extends Logging {

  @transient private var _sparkSession: SparkSession = _

  /**
   * Create a single accessible SparkContext
   *
   * @param inputParams         parsed arguments
   * @param contextName         name of job
   * @param serializerClass     Kryo Registrator used, defaults to SparkRegistrator
   * @param kryoBuffer          kryo buffer, defaults to 8M
   * @param eventLogEnabled     save log files, defaults to false
   * @param applicationSparkLog where to save log files for future access, defaults to file:///tmp/spark-events
   * @param sqlJoinPartitions   how mant partitions to use for DataFrame shuffles
   * @tparam T this is for serializerClass, which is of type Class[T]
   * @return Spark session
   */
  def createSparkSession[T](inputParams:Map[String, String],
                            contextName: String,
                            serializerClass: Class[T],
                            kryoBuffer: String = "8M",
                            eventLogEnabled: Boolean = false,
                            applicationSparkLog: String = "file:///tmp/spark-events",
                            sqlJoinPartitions: Int = 200
                           ): SparkSession = {

    // Set log level for noisy Spark parsers to WARN to avoid INFO messages
    Logger.getLogger("org.apache.spark.sql.catalyst.parser.AbstractParser").setLevel(Level.WARN)

    _sparkSession = if(inputParams("env").toLowerCase == "azure") {
      SparkSession
        .builder()
        .enableHiveSupport()
        .getOrCreate()
    } else {
      val conf = new SparkConf()
      conf
        .setMaster(inputParams("master"))
        .setAppName(contextName)
        .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
        .set("spark.kryo.registrator", serializerClass.getCanonicalName)
        .set("spark.kryoserializer.buffer", kryoBuffer)
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.sql.shuffle.partitions", sqlJoinPartitions.toString)

      logInfo(s"Created new SparkContext named $contextName")

      SparkSession
        .builder()
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
    }

    sparkSession
  }

  def sparkSession: SparkSession = _sparkSession
}