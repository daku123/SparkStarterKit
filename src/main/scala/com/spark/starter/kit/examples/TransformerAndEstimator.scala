package com.spark.starter.kit.examples

import org.apache.spark.sql.SparkSession

object TransformerAndEstimator {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Databricks Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val scaleDf = spark
      .read
      .parquet(
        "C:\\Users\\Vishunath.Sharma\\github\\Spark-The-Definitive-Guide\\data\\simple-ml-scaling"
      )

    scaleDf.show(10)
    scaleDf.printSchema()
    import org.apache.spark.ml.feature.StandardScaler

    val scaler = new StandardScaler().setInputCol("features")

    val result = scaler.fit(scaleDf).transform(scaleDf)
    result.show(false)
    result.printSchema()
  }


}
