package com.spark.starter.kit.examples

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object BasicStructuredOperations {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    booleanExpression(spark)
  }

  def regexOperationOnDataFrame(spark:SparkSession)={

    val dfWithSchema = getDataFrame(spark)
    val country = Seq("India","UNITED STATES","Egypt","Canada")

    val regexString = country.map(cName=>cName.toUpperCase).mkString("|")
    val newRegexString = country.map(cName=>cName.toUpperCase).mkString("(", "|", ")")

    val dfWithRegex = dfWithSchema
                      .select(regexp_replace(
                          upper(col("DEST_COUNTRY_NAME")),
                          regexString,"Pune").alias("country"))
    dfWithRegex.show(4)

    print(newRegexString)
    print(regexString)
    val extractFirstCountry = dfWithSchema
                            .select(regexp_extract(
                                                  upper(col("DEST_COUNTRY_NAME")),
                                                  newRegexString,0)
                                                  .alias("country"),
                                    col("DEST_COUNTRY_NAME"))

    extractFirstCountry.show(4)
  }


  def booleanExpression(spark:SparkSession) = {

    val countPredicate = col("count").gt("1000")
    val countryName = col("ORIGIN_COUNTRY_NAME").equalTo("Egypt")

    val df = getDataFrame(spark).selectExpr("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME","Count")
                                .where(countPredicate.or(countryName))

     df.show(4)

  }

  def createManualSchema():StructType={
    val manualSchema =  StructType(List(StructField("DEST_COUNTRY_NAME",StringType,true),
                        StructField("ORIGIN_COUNTRY_NAME",StringType,true),
                        StructField("count",IntegerType,false)))

    return manualSchema
  }


  def getDataFrame(spark:SparkSession):DataFrame={

    val dfWithSchema = spark.read.format("json").schema(createManualSchema())
                       .load("C:\\Users\\vishunath.sharma\\github"  +
                                    "\\Spark-The-Definitive-Guide\\data\\" +
                                    "flight-data\\json\\2015-summary.json")
    return dfWithSchema
  }

  def getSparkSession():SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
                .config("spark.master", "local").getOrCreate()
  }
}
