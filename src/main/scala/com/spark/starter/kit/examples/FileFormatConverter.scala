package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession

object FileFormatConverter {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    val inputPath = "C:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\csv\\*"
    convertCsvToParquet(spark,inputPath)
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def convertCsvToParquet(spark:SparkSession,csvPath:String)={
    val df = spark.read.format("csv").option("inferschema","true")
                  .option("header","true").load(csvPath)

    val outputPath = "C:\\Users\\Vishunath.Sharma\\github\\SparkStarterKit\\src\\main\\resources\\output"
    df.write.save(outputPath)
  }
}
