package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

object Example {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    getLogDataSet(spark, getClass.getResource("/logFile.csv").getPath)
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  case class LogFile(id: Int, loginId: Int, action: String, userName: String)

  def getLogDataSet(spark: SparkSession, filePath: String) = {
    import spark.implicits._

    val tableFirst = spark.read.format("csv").schema(createManualSchema())
      .load(filePath)

    val filterCondition1 = col("action")==="add-to-cart"
    val filterCondition2 = col("action")==="purchase"

    //tableFirst = tableFirst.selectExpr("*").where(filterCondition1)
    val tableSecond = spark.read.format("csv").schema(createManualSchema())
      .load(filePath)

    val tableThird = spark.read.format("csv").schema(createManualSchema())
      .load(filePath)

  /*  val logDataSet = dfWithSchema.as[LogFile]
    logDataSet.show(3)*/

    val joinCondition = tableFirst.col("id")===tableSecond.col("id")
    val userWithAddToCartAndPurchaseBoth = tableFirst.where(filterCondition1)
                                           .join(tableSecond.withColumn("id2",col("id"))
                                             .where(filterCondition2),joinCondition)
                                           .selectExpr("id2")

     userWithAddToCartAndPurchaseBoth.show(3)
    val listOfObjects = userWithAddToCartAndPurchaseBoth.select("id2").map(x=>x.getInt(0)).collect().toList

    tableThird.selectExpr("id","loginId").where(!col("id").isin(listOfObjects:_*)).show()
  }

  def createManualSchema(): StructType = {
    val manualSchema = StructType(List(StructField("id", IntegerType, true),
      StructField("loginId", IntegerType, true),
      StructField("action", StringType, false),
      StructField("userName", StringType, false)))

    return manualSchema
  }
}
