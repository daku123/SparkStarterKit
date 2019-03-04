package com.spark.starter.kit.examples

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RockPaperScissorsAssignment {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")

    val path = "C:\\Users\\Vishunath.Sharma\\Downloads\\assignment"

   joinOfDataSets(spark,path)

  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def getDataFrame(spark:SparkSession,filePath:String,schema:StructType):DataFrame={

    spark.read.option("sep", "\t").format("csv").schema(schema).load(filePath)
  }

  /*case class Production(productionUnitId:String,batchId:String,itemProduced:String,itemDiscarded:String)
  case class Complaints(invoiceId:Int,defectiveItem:String)
  case class Sales(invoiceId:Int,customerId:String,itemSummery:String,batchId:String)*/

  def getProductionTableSchema():StructType={ StructType(List(StructField("productionUnitId",StringType,true),
                                              StructField("batchId",StringType,true),StructField("itemProduced",StringType,false),
                                              StructField("itemDiscarded",StringType,true)))

  }

  def getSalesTableSchema():StructType={ StructType(List(StructField("invoiceId",IntegerType,true),
                                         StructField("customerId",StringType,true),
                                         StructField("itemSummery",StringType,false),StructField("batchId",StringType,true)))
  }

  def getComplaintTableSchema():StructType={ StructType(List(StructField("invoiceId",IntegerType,true),StructField("defectiveItem",StringType,true)))
  }


  def joinOfDataSets(spark:SparkSession,filePath:String)={

    import spark.implicits._
    val productionData = getDataFrame(spark,filePath+"\\Production_logs.tsv",getProductionTableSchema())
    val complaintsData = getDataFrame(spark,filePath+"\\Complaints.tsv",getComplaintTableSchema())
    val salesData = getDataFrame(spark,filePath+"\\Sales.tsv",getSalesTableSchema())

    val productionAndSalesJoin = productionData.join(salesData,"batchId")
    val allThreeJoined = complaintsData.join(productionAndSalesJoin,"invoiceId")

    val getCount = udf((colValue:String,pattern:String) =>{
      val value = s"$pattern':\\s+[0-9]*".r
     if (colValue.contains(pattern)) value.findFirstIn(colValue).mkString.split(":")(1).trim.toInt
      else 0
    })

    val countOfComplainedItem = getCount(col("itemSummery"),col("defectiveItem"))
    val countOfDiscardedItem = getCount(col("itemDiscarded"),col("defectiveItem"))
    val totalItem = getCount(col("itemProduced"),col("defectiveItem"))

  //  allThreeJoined.where(col("invoiceId")=!=872013).show(10)
   allThreeJoined.select(col("itemProduced"),totalItem,
                         col("itemDiscarded"),countOfDiscardedItem,
                         col("itemSummery"),countOfComplainedItem,
                         col("defectiveItem"))
                 .show(10)
  //  println(extractValue("{'rock': 49,'paper':120}","rock"))
  }

  def extractValue(colValue:String,pattern:String):Int={

      val value = s"$pattern':\\s+[0-9]*".r
    if (colValue.contains(pattern)) value.findFirstIn(colValue).mkString.split(":")(1).trim.toInt
    else 0
  }


}
