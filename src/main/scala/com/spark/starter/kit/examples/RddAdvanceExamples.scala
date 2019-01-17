package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession

object RddAdvanceExamples {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")

    aggrFunc(spark)
  }
  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def keyByMethod(spark:SparkSession)={
    val words = "C:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\csv".split("\\\\")
    val keyWords = spark.sparkContext.parallelize(words,2)

    keyWords.keyBy(key => key.charAt(0)).collect().foreach(println)
  }

  def mapOverValues(spark:SparkSession)={
    val words = "C:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\csv\\*".split("\\\\")
    val rdd = spark.sparkContext.parallelize(words,2)

    val keyWords = rdd.keyBy(key => key.charAt(0))
    keyWords.mapValues(value => value.toUpperCase).collect().foreach(println)
    keyWords.lookup('c').foreach(println)
  }

  def samplingData(spark:SparkSession)={
    // an RDD[(K, V)] of any key value pairs
    val data = spark.sparkContext.parallelize(Seq((1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')))

    // specify the exact fraction desired from each key
    val fractions = Map(1 -> 0.1, 2 -> 0.6, 3 -> 0.3)

    // Get an approximate sample from each stratum
    val approxSample = data.sampleByKey(withReplacement = false, fractions = fractions)
    approxSample.collect().foreach(println)
  }

  def aggrFunc(spark:SparkSession)={
    val words = "C:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\flight-data\\csv\\*".split("\\\\")
    val rdd = spark.sparkContext.parallelize(words,2)

    val keyValueChars = rdd.map(record =>(record.toLowerCase.charAt(0),1))

    println("count by key operation")
    keyValueChars.countByKey().foreach(println)
    println("group by key")
    keyValueChars.groupByKey().map(record =>(record._1,record._2.reduce(sumFunc))).collect().foreach(println)
    println("reduce by key")
    keyValueChars.reduceByKey((x,y)=>x+y).collect().foreach(println)

    println("combine by key")

    val initialScores = List(
      studentDetails("Fred","maths", 88.0f),
      studentDetails("Fred","Hindi", 95.0f),
      studentDetails("Fred","english", 91.0f),
      studentDetails("Wilma","maths", 93.0f),
      studentDetails("Wilma","hindi", 95.0f),
      studentDetails("Wilma","english", 98.0f))



    val keyValueData = for(i<-initialScores) yield (i.name,i)
    val newRdd = spark.sparkContext.parallelize(keyValueData,2).cache()

    newRdd.combineByKey(combine,combineValue,mergeValue)
      .map(record => (record._1,record._2._1/record._2._2)).collect.foreach(println)

  }

  def maxFunc(left:Int,right:Int)=Math.max(left,right)
  def sumFunc(left:Int,right:Int)=left+right

  case class studentDetails(name:String, subject:String, marks:Float)
  // combine by key functions

  // 1. create combine function
  def combine(x:studentDetails) = (x.marks,1)

  // 2. create combine value within each partition
  def combineValue(keyValue:(Float,Int),x:studentDetails)=(keyValue._1+x.marks,keyValue._2+1)

  //3. merge all the values from all the partitions
  def mergeValue(keyValue1:(Float,Int),keyValue2:(Float,Int)) = (keyValue1._1+keyValue2._1,keyValue1._2+keyValue2._2)

}
