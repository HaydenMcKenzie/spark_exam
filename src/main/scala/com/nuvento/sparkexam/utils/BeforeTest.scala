package com.nuvento.sparkexam.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BeforeTest {
  def createDataFrameAndReadInData(spark: SparkSession, fileName: String): DataFrame = {
    spark.read.option("header", "true").csv("./data/" + fileName + ".csv")
  }

  def sparkSessionSetUp(): SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .appName("YourSparkTest")
      .master("local[2]")
      .getOrCreate()

    spark
  }
}
