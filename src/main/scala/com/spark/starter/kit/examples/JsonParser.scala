package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object JsonParser {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val jsonString = "{\"id\":\"int\",\"name\":\"string\",\"city\":\"string\",\"pin\":\"int\",\"tsmt\":\"string\"}"

    val arrayOfColAndType = jsonString.substring(1,jsonString.length-1).split(",")
    val listOfStructFields = new ListBuffer[StructField]

   for (colAndType <- arrayOfColAndType){

    listOfStructFields += StructField(colAndType.split(":")(0),matcher(colAndType.split(":")(1)),true)
   }

    def matcher(pattern:String) = {
      pattern match {
        case "\"string\"" => StringType
        case "\"int\"" => IntegerType
      }
    }
    println(listOfStructFields)
    println(StructType(listOfStructFields.toList))

   val spark = getSparkSession()
    val read = spark.read.textFile("C:\\Users\\Vishunath.Sharma\\Downloads\\schema.json")

    var schema = ""
    read.collect().foreach{
      line => schema+=line
    }

    println(schema)
    }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("UsertUsingIndexTable").config("spark.master","local")
      .getOrCreate()

  }
  }


