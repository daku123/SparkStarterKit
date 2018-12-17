package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession

object DBConnection {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    getDfFromDataBase(spark)
  }

  def getDfFromDataBase(spark:SparkSession)={
    val path = "C:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\jdbc\\my-sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val driver = "org.sqlite.JDBC"
    val dtable = "flight_info"

    val df = spark.read.format("jdbc")
                  .option("url",url)
                  .option("driver",driver)
                  .option("dbtable",dtable)
                  .load()

    df.show(4)
  }
  def getSparkSession():SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

}
