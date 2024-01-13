package com.nuvento.sparkexam.TestQuestionOne

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestingJoin extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("YourSparkTest")
    .master("local[2]")
    .getOrCreate()

  import DFCreationLogic._

  test("Test join operation on imported data") {
    val customerInfoDF: DataFrame = createCustomerInfoDataFrame(spark)
    val accountInfoDF: DataFrame = createAccountInfoDataFrame(spark)

    val resultDF: DataFrame = JoinedLogic.joinDataFrames(customerInfoDF, accountInfoDF)

    val expectedDF: DataFrame = customerInfoDF.join(accountInfoDF, "customerId")

    // Collect rows from DataFrames
    val resultRows = resultDF.collect()
    val expectedRows = expectedDF.collect()

    println("resultRows = " + resultRows.take(5).mkString(", "))
    println("expectedRows = " + expectedRows.take(5).mkString(", "))

    assert(resultRows.sameElements(expectedRows))
  }
}

object DFCreationLogic {
  def createCustomerInfoDataFrame(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").csv("./data/customer_data.csv")
  }

  def createAccountInfoDataFrame(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").csv("./data/account_data.csv")
  }
}

object JoinedLogic {
  def joinDataFrames(customerInfoDF: DataFrame, accountInfoDF: DataFrame): DataFrame = {
    customerInfoDF.join(accountInfoDF, "customerId")
  }
}
