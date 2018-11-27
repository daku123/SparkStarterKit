package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


object StructuredStreamingExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.master","local")
      .getOrCreate()
     import spark.implicits._
     spark.conf.set("spark.sql.shuffle.partitions", "5")

    val retailData = spark.read.option("inferschema","true")
      .option("header","true")
      .csv("C:\\Users\\vishunath.sharma\\github\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\*.csv")



    retailData.createGlobalTempView("retail_data")
     val retailSchema = retailData.schema
    /*retailData.selectExpr("CustomerId","(UnitPrice*Quantity) as total_cost","InvoiceDate").groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
      .sum("total_cost").show(5)


    retailData.selectExpr("CustomerId","(UnitPrice*Quantity) as total_cost","InvoiceDate","UnitPrice","Quantity")
              .groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
              .agg(max(col("UnitPrice")),min("Quantity"))
              .show(5)
*/
  /*
  read the data as streaming. The above code is for batch data.
   */
    streamingData(spark,retailSchema)
    //streamingDataNew(spark)
  }

  /**
    *  Currently this method is failing because of java.lang.IllegalStateException:
    *  Cannot call methods on a stopped SparkContext.
    * @param spark
    * @param staticSchema
    * @return
    */
  def streamingData(spark:SparkSession,staticSchema:StructType)={

    val realTimeStreamingData = spark.readStream
      .schema(staticSchema)
      .option("inferschema","true")
      .option("header","true")
      .option("maxFilesPerTrigger","1")
      .csv("C:\\Users\\vishunath.sharma\\github\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\*.csv")

     realTimeStreamingData.isStreaming

     val purchaseByCustomerPerHour = realTimeStreamingData
                                     .selectExpr("CustomerId","(UnitPrice*Quantity) as total_cost","InvoiceDate")
                                     .groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
                                     .sum("total_cost")

     purchaseByCustomerPerHour
                              .writeStream.format("console")
                              .queryName("customer_purchases")
                              .outputMode("complete")
                              .start()
  }
}
