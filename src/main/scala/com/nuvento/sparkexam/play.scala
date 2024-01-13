package com.nuvento.sparkexam


import org.apache.log4j._
import org.apache.spark.sql._

object play extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    def loadCustomerData(spark: SparkSession, dataFile: String): DataFrame = {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(dataFile)
  }
}

