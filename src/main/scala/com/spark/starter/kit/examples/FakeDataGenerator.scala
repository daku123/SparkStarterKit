package com.spark.starter.kit.examples

import java.io.{File, PrintWriter}

import scala.util.Random

object FakeDataGenerator {

  def main(args: Array[String]): Unit = {
    val names = Array("vishu","Aman","Parikshit","Sathe","Daya")
    val cities = Array("pune","UP","UP","MUMBAI","LATUR")
    val pin = Array(1234,4567,12345,98765,2345)
    val empDate = Array("2012-01-01","2013-01-01","2014-01-01","2015-01-01","2016-01-01")

    val writer = new PrintWriter(new File("fakeData.csv"))
    for( i <- 1 to 1000000){
      val num = new Random().nextInt(5)
      val str = i.toString+","+names(num)+","+cities(num)+","+pin(num)+","+empDate(num)+"\n"
      writer.write(str)
    }
    writer.close()
  }
}
