package com.nuvento.sparkexam.TestQuestionOne

import com.nuvento.sparkexam.utils.SQLFunctions._
import com.nuvento.sparkexam.utils.BeforeTest._
import com.nuvento.sparkexam.utils.Functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class TestingLogicFromData extends AnyFlatSpec {
  // What is getting tested
  val inputDF: DataFrame = createDataFrameAndReadInData(sparkSessionSetUp(), "customer_data")
  val resultDF: DataFrame = selectColumnsFromData(inputDF)

  // Test Logic
  it should "match schemas" in {
    assertDataFrameSchemaEquals(inputDF, resultDF)
  }
  it should "match lengths" in {
    assertDataFrameLengthEquals(inputDF, resultDF)
  }
  it should "match sets" in {
    assertDataFrameSetEquals(inputDF, resultDF)
  }
}