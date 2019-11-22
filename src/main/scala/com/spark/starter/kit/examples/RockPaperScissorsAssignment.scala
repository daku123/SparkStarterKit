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

    //report 1-->

    val getCount = udf((colValue:String,pattern:String) =>{
      val value = s"$pattern':\\s+[0-9]*".r
     if (colValue.contains(pattern)) value.findFirstIn(colValue).mkString.split(":")(1).trim.toInt
      else 0
    })

    val countOfComplainedItem = getCount(col("itemSummery"),col("defectiveItem"))
    val countOfDiscardedItem = getCount(col("itemDiscarded"),col("defectiveItem"))
    val totalItem = getCount(col("itemProduced"),col("defectiveItem"))
    val additionOfTwoItem = countOfDiscardedItem + countOfComplainedItem
    val finalResult = additionOfTwoItem*100/totalItem

    val countOfPaper = getCount(col("itemSummery"),col("paper"))
    val countOfRock = getCount(col("itemSummery"),col("rock"))
    val countOfScissor = getCount(col("itemSummery"),col("scissor"))

    allThreeJoined.select(col("productionUnitId"),
      col("defectiveItem"), bround(finalResult,2).alias("finalResult"),col("batchId")).show(50)



    /// report 2 :-
    val salesDataWithExtraColumn = salesData
      .withColumn("paper",lit("paper"))
      .withColumn("rock",lit("rock"))
      .withColumn("scissor",lit("scissor"))
      .withColumn("countOfPaper",countOfPaper)
      .withColumn("countOfRock",countOfRock)
      .withColumn("countOfScissor",countOfScissor)

    val salesAndComplaintsJoin = salesDataWithExtraColumn.join(complaintsData.withColumn("invoiceId2",col("invoiceId")),Seq("invoiceId"),"left_outer")


    salesAndComplaintsJoin.withColumn("cstGrp",col("customerId").substr(0,1)).groupBy($"cstGrp")
      .agg(
        count("invoiceId2").alias("numberOfComplaints"),
        sum($"countOfPaper").alias("paper"),
        sum($"countOfRock").alias("rock"),
        sum($"countOfScissor").alias("scissor"))
      .show(20)

    // report 3:---
    val getSumOfDefectedItem = udf((colValue:String) =>{

      var sum:Long = 0;
    val stripString = colValue.substring(1,colValue.length-1)
    val arrayOfItems = stripString.split(",")
      arrayOfItems.foreach(item=> sum+=item.split(":")(1).trim.toLong)
      sum
    }
    )

    val totalDefectedItemAtFactory = getSumOfDefectedItem(col("itemDiscarded"))
    val defectedPrecent = max(totalDefectedItemAtFactory)*100/(sum($"customerReported")+max(totalDefectedItemAtFactory))
    allThreeJoined.withColumn("defectedItem",totalDefectedItemAtFactory)
      .withColumn("customerReported",countOfComplainedItem).groupBy("productionUnitId")
      .agg(bround(defectedPrecent,2).alias("defectedPrecent")).show(20)

  }

}
