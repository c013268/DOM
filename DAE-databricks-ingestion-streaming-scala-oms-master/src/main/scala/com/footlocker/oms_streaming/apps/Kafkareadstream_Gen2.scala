package com.footlocker.oms_streaming.apps

import org.apache.spark.sql.{DataFrame, SparkSession}


object Kafkareadstream_Gen2 {

  /**
   * ================================= READ STREAM FROM KAFKA ================================
   */


  def readFromKafkaStream(spark: SparkSession, inputParams: Map[String, String]): DataFrame = {

    val kafkaClusterApiKey = inputParams("kafkaClusterApiKey")
    val kafkaClusterApiSecret = inputParams("kafkaClusterApiSecret")
    val read_mode = inputParams("read_mode")

    val oms_Stream_raw = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", inputParams("kafka_servers"))
        .option("subscribe", inputParams("oms_topic"))
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", s"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='${kafkaClusterApiKey}' password='${kafkaClusterApiSecret}';""")
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", s"""${inputParams("oms_topic")}_oms_ingestion_AdlsLoadMain_Gen2""")
        .option("failOnDataLoss", "false")
        //.option("maxOffsetsPerTrigger", inputParams("offset_limit"))
        .load()

    oms_Stream_raw.selectExpr("CAST(value AS STRING) as json", "timestamp as load_time_kafka", "to_timestamp(now(),'yyyy-MM-dd HH:mm:dd') as load_time_adls")
      .select("*")
  }

  }
