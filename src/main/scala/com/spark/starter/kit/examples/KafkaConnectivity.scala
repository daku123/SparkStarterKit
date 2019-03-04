package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession


object KafkaConnectivity {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    readingFromKafka(getSparkSession())
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def readingFromKafka(spark:SparkSession)={

    val dataFromKafka = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092").option("subscribe","northbound")
      .option("startingoffsets","latest")
      .option("enable.auto.commit","false").load()
      .selectExpr("CAST(value AS STRING)")

    val stream = dataFromKafka.select("*").writeStream.format("console").outputMode("append").start()

    stream.awaitTermination()
  }
}
