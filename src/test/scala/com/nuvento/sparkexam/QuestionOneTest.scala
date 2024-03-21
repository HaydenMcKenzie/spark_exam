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
    val testMainProgram = questionOne(customerData, accountData, "customerId", "left")

    // Actual
    val actual = testMainProgram.schema

    // Except
    val expect = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("numberAccounts", IntegerType, nullable = false),
      StructField("totalBalance", LongType, nullable = true),
      StructField("averageBalance", DoubleType, nullable = true)
    ))

    // Test
    assert(actual == expect)
  }
}