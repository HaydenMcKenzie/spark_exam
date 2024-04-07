package com.nuvento.sparkexam

import com.nuvento.sparkexam.QuestionOne._
import com.nuvento.sparkexam.SetUp.{accountData, customerData}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class QuestionOneTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  import com.nuvento.sparkexam.utils.SparkSetup.spark.implicits._

  test("Testing QuestionOne Main Program") {
    val testMainProgram = questionOne(customerData,accountData)

    // Actual
    val actual = testMainProgram.schema

    // Except
    val expect = StructType(Array(
      StructField("customerId", StringType, true),
      StructField("forename", StringType, true),
      StructField("surname", StringType, true),
      StructField("accounts", ArrayType(
        StructType(Array(
          StructField("customerId", StringType, true),
          StructField("accountId", StringType, true),
          StructField("balance", IntegerType, true)
        )),
        false
      )),
      StructField("numberAccounts", IntegerType, false),
      StructField("totalBalance", LongType, true),
      StructField("averageBalance", DoubleType, true)
    ))

    // Test
    assert(actual == expect)
  }
}