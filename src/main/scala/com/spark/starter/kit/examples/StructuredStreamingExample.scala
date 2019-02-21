package com.spark.starter.kit.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


object StructuredStreamingExample {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    streamingData(spark)
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  /**
    *  Currently this method is failing because of java.lang.IllegalStateException:
    *  Cannot call methods on a stopped SparkContext.
    * @param spark
    * @param staticSchema
    * @return
    */
  def streamingData(spark:SparkSession)={

    val staticJsonData = spark.read.format("json")
                   .load("c:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\activity-data")

    val streamData = spark.readStream.schema(getSchema(staticJsonData))
                     .option("maxFilesPerTrigger",1)
                     .json("c:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\activity-data")

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    val countQuery = streamData.writeStream.format("memory").queryName("count_analysis")
              .outputMode("append").start()
   // streamData.writeStream.outputMode("append").format("console").start().awaitTermination()
    //spark.conf.set("spark.sql.shuffle.partitions", 5)

streamData.groupBy("gt").count().writeStream.format("console")
          .outputMode("complete").start().awaitTermination()
    countQuery.awaitTermination()


  }

  def getSchema(dataFrame:DataFrame):StructType={return dataFrame.schema}
}
