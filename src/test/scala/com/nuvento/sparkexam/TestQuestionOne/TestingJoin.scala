package com.nuvento.sparkexam.TestQuestionOne

import com.nuvento.sparkexam.TestQuestionOne.holding.SQLFunctions._
import com.nuvento.sparkexam.TestQuestionOne.holding.BeforeTest._
import com.nuvento.sparkexam.TestQuestionOne.holding.TestingFunctions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class TestingJoin extends AnyFlatSpec {
  val customerInfoDF: DataFrame = createDataFrameAndReadInData(sparkSessionSetUp(), "customer_data")
  val accountInfoDF: DataFrame = createDataFrameAndReadInData(sparkSessionSetUp(), "account_data")

  val joinedInfoDf: DataFrame = joinDataFrames(customerInfoDF, accountInfoDF, "customerId")
  val expectedDF: DataFrame = customerInfoDF.join(accountInfoDF, "customerId")

  it should "match Schema" in {
    assertDataFrameSchemaEquals(joinedInfoDf, expectedDF)
  }
  it should "match lengths" in {
    assertDataFrameLengthEquals(joinedInfoDf, expectedDF)
  }
  it should "match sets" in {
    assertDataFrameSetEquals(joinedInfoDf, expectedDF)
  }
}

