package com.spark.starter.kit.examples

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinOperations {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    joinOperation(spark)
  }

  def getDataFrame(spark:SparkSession):DataFrame = {
    val dfWithSchema = spark.read.format("json").schema(createManualSchema())
                      .load("C:\\Users\\vishunath.sharma\\github"  +
                                   "\\Spark-The-Definitive-Guide\\data\\" +
                                   "flight-data\\json\\2015-summary.json")
    return dfWithSchema
  }

  def createManualSchema():StructType={
    val manualSchema =  StructType(List(StructField("DEST_COUNTRY_NAME",StringType,true),
      StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      StructField("count",IntegerType,false)))

    return manualSchema
  }

  def joinOperation(spark:SparkSession)={
    val leftDf = getDataFrame(spark)
    val rightDf = getDataFrame(spark)
    val joinExpression = leftDf.col("count")===rightDf.col("count")
    val result = leftDf.join(rightDf,joinExpression,"inner")
    result.show(6)
  }
  def getSparkSession():SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }
}
