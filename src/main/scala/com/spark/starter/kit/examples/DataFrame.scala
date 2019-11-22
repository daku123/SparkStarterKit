package com.spark.starter.kit.examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrame  {

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Databricks Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    usingDataFrameApi(spark)
    dataFrameOperations(spark)

  }

  def usingDataFrameApi(spark:SparkSession)={
    val flightData = spark.read.option("inferschema","true").option("header","true").csv("C:\\Users\\vishunath.sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\csv")

   //creating a temp table on flightData.
    flightData.createOrReplaceTempView("flight_data_2010")
    flightData.select( max("count")).show()
    flightData.groupBy("DEST_COUNTRY_NAME")
              .max("count")
              .show()

    //flightData.groupBy("DEST_COUNTRY_NAME").sum("count").sort(desc("sum(count)")).limit(5).show()
    flightData.groupBy("DEST_COUNTRY_NAME")
              .sum("count")
              .sort(desc("sum(count)"))
              .limit(5).show()
  }

  def readJsonUsingDataFrame(spark:SparkSession)={

    val jsonDataFrame =  spark.read
                        .format("json")
                        .load("C:\\Users\\vishunath.sharma\\github\\" +
                          "Spark-The-Definitive-Guide\\data\\flight-data\\json\\" +
                          "2015-summary.json")
    jsonDataFrame.printSchema()

    //creating manual schema for the given dataset
    val manualSchema = StructType(List(StructField("DEST_COUNTRY_NAME",StringType,true),
                       StructField("ORIGIN_COUNTRY_NAME",StringType,true),
                       StructField("count",LongType,true,Metadata.fromJson("{\"hello \":\"world\"}"))))

    val dfWithSchema = spark.read
                       .format("json")
                       .schema(manualSchema)
                       .load("C:\\Users\\vishunath.sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2015-summary.json")

     dfWithSchema.limit(4).show()
  }

  def createDataFrameFromRows(spark:SparkSession)={
    val rows = Seq(Row("India","US",1202L),Row("India","Russia",334L))
    val rdd = spark.sparkContext.parallelize(rows)
    val dataFrame = spark.createDataFrame(rdd,createManualSchema())
    dataFrame.show()
  }

  def createManualSchema():StructType={
    val manualSchema =  StructType(List(StructField("DEST_COUNTRY_NAME",StringType,true),
       StructField("ORIGIN_COUNTRY_NAME",StringType,true),
       StructField("count",IntegerType,false)))

    return manualSchema
  }

  def dataFrameOperations(spark:SparkSession)={
    val dfWithSchema = spark.read
      .format("json")
      .schema(createManualSchema())
      .load("C:\\Users\\vishunath.sharma\\" +
        "github\\Spark-The-Definitive-Guide\\data\\" +
        "flight-data\\json\\2015-summary.json")

     dfWithSchema.printSchema()
    dfWithSchema.show(1)

     dfWithSchema
                 .selectExpr("ORIGIN_COUNTRY_NAME","(ORIGIN_COUNTRY_NAME=DEST_COUNTRY_NAME) as flag")
                 .show(3)


    //Adding a column to existing schema
    val dfWithExtraColumn = dfWithSchema
                            .withColumn("flag",expr("ORIGIN_COUNTRY_NAME=DEST_COUNTRY_NAME"))


    dfWithExtraColumn.printSchema()

    //casting of column
    dfWithSchema.withColumn("count2",exp(col("count"))
                .cast(StringType)).show(4)


    // removing column and applying filter to dataframe.
    val dfWithDroppedColumn = dfWithExtraColumn.drop("count2")
    dfWithDroppedColumn.printSchema()

    dfWithExtraColumn.where((col("Count").isNotNull)
                       .and(col("DEST_COUNTRY_NAME")
                         .equalTo("United States")))
                     .show(10)

  }

  def readJson(spark:SparkSession)={
    val data = spark.read.json("C:\\Users\\vishunath.sharma\\github" +
                                      "\\Spark-The-Definitive-Guide\\data\\" +
                                      "flight-data\\json\\2015-summary.json")
    data.show(4)
  }
}