package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame extends Serializable {

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Databricks Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val authors = Seq("bill,databricks", "matei,databricks")
    authors.toDF("colmn1").show()
usingDataFrameApi(spark)
  }

  def usingDataFrameApi(spark:SparkSession)={
    val flightData = spark.read.option("inferschema","true").option("header","true").csv("C:\\Users\\vishunath.sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\csv")
    /*
   creating a temp table on flightData.
   */
    flightData.createOrReplaceTempView("flight_data_2010")
    flightData.select( max("count")).show()
    flightData.groupBy("DEST_COUNTRY_NAME").max("count").show()
    //flightData.groupBy("DEST_COUNTRY_NAME").sum("count").sort(desc("sum(count)")).limit(5).show()
    flightData.groupBy("DEST_COUNTRY_NAME").sum("count").sort(desc("count")).limit(5).show()
  }
}