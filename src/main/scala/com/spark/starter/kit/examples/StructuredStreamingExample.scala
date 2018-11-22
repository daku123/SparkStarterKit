package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object StructuredStreamingExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.master","local")
      .getOrCreate()

    val retailData = spark.read.option("inferschema","true")
      .option("header","true")
      .csv("C:\\Users\\vishunath.sharma\\github\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\*.csv")
    import spark.implicits._

    retailData.createGlobalTempView("retail_data")
     val retailSchema = retailData.schema
    retailData.selectExpr("CustomerId","(UnitPrice*Quantity) as total_cost","InvoiceDate").groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
      .sum("total_cost").show(5)

    retailData.selectExpr("CustomerId","(UnitPrice*Quantity) as total_cost","InvoiceDate","UnitPrice","Quantity")
              .groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
              .agg(max(col("UnitPrice")),min("Quantity"))
              .show(5)
  }
}
