package com.spark.starter.kit.examples

import org.apache.spark.sql.{Row, SparkSession}

object RddExample {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    readJsonFile(spark)
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def readJsonFile(spark:SparkSession)={

    import spark.implicits._
    val flightData = spark.read.textFile("c:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2012-summary.json")

    print(flightData.toDF())
   // val arrayOfData = flightData.map(record => record.split(",")).toDF().map(x=>x.getString(0)).collect().toList
    val arrayOfData:Seq[Row] = flightData.toDF().collect().toList
    val rdd = spark.sparkContext.parallelize(arrayOfData,2)

    val rdd2 = flightData.toDF().rdd.map(x=> x.getString(0)).take(4)
    //print(rdd2.foreach(println))
   // rdd.collect().foreach(println)
   // rdd.collect().map(x=>x.getString(0)).take(4).foreach(println)

    val string = "c:,Users,Vishunath.Sharma,github,Spark-The-Definitive-Guide,data,flight-data,json,2012-summary.json".split(",")

    val newRdd = spark.sparkContext.parallelize(string,2)

    val words = newRdd.map(x=> (x,x(0),x.startsWith("S")))
    print(words.collect().foreach(println))
    print(newRdd.sortBy(x=>x.length()* 1).foreach(println))

    println("iterating partitions of rdd")
    newRdd.mapPartitionsWithIndex(indexFunc).collect().foreach(println)

    println("foreach method for iterating partitions of rdd")

    newRdd.foreachPartition{ iter =>
      import java.io._
      import scala.util.Random

      val fileName = new Random().nextInt()
      val fileObject = new PrintWriter(
                       new File(s"C:\\Users\\Vishunath.Sharma\\github\\SparkStarterKit\\src\\main\\resources\\${fileName}.txt"))

      while (iter.hasNext){
        fileObject.write(iter.next())
      }
      fileObject.close()
    }

  }

  def indexFunc(partitionIndex:Int,partitionIterator:Iterator[String])={
    partitionIterator.toList.map(value => s"partition: $partitionIndex =>$value").iterator
  }

}
