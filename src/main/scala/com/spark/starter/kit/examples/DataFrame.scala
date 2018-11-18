package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession

object DataFrame extends Serializable {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Databricks Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val authors = Seq("bill,databricks", "matei,databricks")
    authors.toDF("colmn1").show()

  }

  def usingDataFrameApi(spark:SparkSession)={
    var flightData = spark.read.option("inferschema","true").option("header","true").csv("/home/ashtro/vishu/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")

    /*
   creating a temp table on flightData.
   */
    flightData.createOrReplaceTempView("flight_data_2010")
    //flightData.select( {'max":'count'}).show()
    //flightData.agg().
    //flightData.groupBy("DEST_COUNTRY_NAME").sum("count").sort(desc("sum(count)")).limit(5).show()

  }
}