package com.spark.starter.kit.examples


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object Example2 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    //getLogDataSet(spark, getClass.getResource("/logFile.csv").getPath)
    loadData(spark)
  }


  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def loadData(spark:SparkSession)={

    val employeeData1 = spark.read.option("inferschema","true")
      .option("header","true")
      .csv("C:\\Users\\Vishunath.Sharma\\github\\SparkStarterKit\\src\\main\\resources\\file1.csv")

    val employeeData2 = spark.read.option("inferschema","true")
      .option("header","true")
      .csv("C:\\Users\\Vishunath.Sharma\\github\\SparkStarterKit\\src\\main\\resources\\file2.csv")

    import spark.implicits._

    val unionOfData = employeeData1.union(employeeData2)

    val results = unionOfData.groupBy("employee_name","employee_id").count.filter($"count"===1)

      //.where(col("cnt").gt(1))

   // val results = unionOfData.withColumn("cnt",count("*"))
    results.selectExpr("employee_name","employee_id").show()
  }

  case class Flight(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:BigInt)
}

