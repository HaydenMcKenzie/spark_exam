package com.nuvento.sparkexam.TestQuestionOne

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestingGroupByColumnCount extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("YourSparkTest")
    .master("local[2]")
    .getOrCreate()

  import DataFrameCreationLogic._

  test("Test select operation on imported data") {
    val inputDF: DataFrame = createDataFrame(spark)

    val resultDF: DataFrame = SelectLogic.selectColumns(inputDF)

    val expectedDF: DataFrame = inputDF.select("customerId", "forename", "surname")

    // Collect rows from DataFrames
    val resultRows = resultDF.collect()
    val expectedRows = expectedDF.collect()

    println("resultRows = " + resultRows.take(5).mkString(", "))
    println("expectedRows = " + expectedRows.take(5).mkString(", "))

    assert(resultRows.sameElements(expectedRows))
  }
}

object DataFrameCreationLogic {
  def createDataFrame(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").csv("./data/customer_data.csv")
  }
}

object SelectLogic {
  def selectColumns(inputDF: DataFrame): DataFrame = {
    inputDF.select("customerId", "forename", "surname")
  }
}
