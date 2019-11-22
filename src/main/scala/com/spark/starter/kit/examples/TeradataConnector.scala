package com.spark.starter.kit.examples

import org.apache.spark.sql.{DataFrame, SparkSession}

object TeradataConnector {
  //EDW_PROD.FEDERATED.FDS
  val DRIVER  = "com.teradata.jdbc.TeraDriver"
  //val CONN_URL = "jdbc:teradata://11.48.230.208/database=RRPMST"
  val CONN_URL = "jdbc:teradata://11.120.29.28/database=RRLMST"
  val DBTABLE = "rgstry_dim_max"
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = this.getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    connectToTeraData(spark)
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Spark Example")
      .config("spark.master", "local").getOrCreate()
  }

  def connectToTeraData(spark:SparkSession): Unit ={

    val numPartitions = 10
    //val df: Array[DataFrame] = new Array[DataFrame](numPartitions)
     val df = spark.read.format("jdbc")
      .option("url",CONN_URL)
      .option("driver",DRIVER)
      .option("dbtable",DBTABLE)
      .option("user","GCPHDPRON01")
      .option("password","UJhka54Koi0")
      //.option("partitionColumn","rgstry_dim_id")
      //.option("lowerBound",lowerBound(spark,"rgstry_dim_id"))
      //.option("upperBound",upperBound(spark,"rgstry_dim_id"))
      .option("numPartitions",numPartitions)
      .load()

    print(df.show(10))

    loadTOBigQuery(spark,df)
  }

def loadTOBigQuery(spark:SparkSession,df:DataFrame){

  //val bucket = spark.sparkContext.hadoopConfiguration.get("fs.gs.system.bucket")
  //val bucket = "gs://mtech-daas-product-pdata_temp"
  val bucket = "gs://us-east1-edw-dev-pipeline-c0c0f08c-bucket"
  spark.conf.set("temporaryGcsBucket", bucket)
    df.write.format("bigquery")
    .option("table","mtech-daas-product-pdata-dev:transient.rgstry_dim_max_trunc")
    .save()
}

  def lowerBound(spark:SparkSession,lowerBoundCol:String):Int={
    spark.read.format("jdbc")
      .option("url",CONN_URL)
      .option("driver",DRIVER)
      .option("dbtable",s"(select min(rgstry_dim_id) from rrpmst.${DBTABLE})")
      .option("user","GCPHDPRON01")
      .option("password","UJhka54Koi0")
      .load().first().getInt(0)

  }

  def upperBound(spark:SparkSession,lowerBoundCol:String):Int={
     spark.sql(s"select max(${lowerBoundCol}) from rrpmst.${DBTABLE}").first().getInt(0)
  }


}
