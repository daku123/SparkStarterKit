package com.spark.starter.kit.examples

import org.apache.spark.{SparkConf, SparkContext}

object WordCountProblem extends Serializable {
  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc:SparkContext = new SparkContext(conf)
    val file = sc.textFile("/home/ashtro/vishu/sampleData.txt")
    val splitData = file.flatMap(record => record.split(","))
    val pair = splitData.map(x=>(x,1))
    val grp = pair.reduceByKey((x,y)=>(x+y))
    grp.collect.foreach(println)
  }
}
