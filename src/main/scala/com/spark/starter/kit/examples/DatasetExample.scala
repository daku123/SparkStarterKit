package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession
object DatasetExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
   val spark = SparkSession.builder()
               .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
               .config("spark.master","local")
               .getOrCreate()
    val flightData = spark.read.option("inferschema","true")
                     .option("header","true")
                     .csv("C:\\Users\\vishunath.sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\csv")
    import spark.implicits._

    val flightDataset = flightData.as[Flight]

    flightDataset.filter(record => record.DEST_COUNTRY_NAME!="canada").show(5)
    flightDataset.filter(record=>record.DEST_COUNTRY_NAME!="canada")
      .map(data=>Flight(data.DEST_COUNTRY_NAME,data.ORIGIN_COUNTRY_NAME ,data.count + 5)).show(5)
  }

  case class Flight(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:BigInt)
}
