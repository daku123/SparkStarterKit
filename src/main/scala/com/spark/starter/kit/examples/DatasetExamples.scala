package com.spark.starter.kit.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DatasetExamples {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    getDataSet(spark)
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def createManualSchema(): StructType = {
    val manualSchema = StructType(List(StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", IntegerType, false)))

    return manualSchema
  }

  case class Flights(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

  def getDataSet(spark: SparkSession) = {
    import spark.implicits._
    val dfWithSchema = spark.read.format("json")
      .load("C:\\Users\\vishunath.sharma\\github" +
        "\\Spark-The-Definitive-Guide\\data\\" +
        "flight-data\\json\\2015-summary.json")
    val flightDataSet = dfWithSchema.as[Flights]
    flightDataSet.filter(dataSet => filterOperation(dataSet)).show(4)

    val boolExpr = flightDataSet.col("count").gt(30)
    val iterator = Seq(flightDataSet)
  }

  def filterOperation(dataSet: Flights): Boolean = {
    return dataSet.DEST_COUNTRY_NAME == dataSet.ORIGIN_COUNTRY_NAME
  }

}
