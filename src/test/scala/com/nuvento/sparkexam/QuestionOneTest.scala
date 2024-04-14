package com.nuvento.sparkexam

// Nuvento Imports
import com.nuvento.sparkexam.QuestionOne._
import com.nuvento.sparkexam.SetUp.{accountData, customerData}
import com.nuvento.sparkexam.handlefiles.Schemas.CustomerAccountOutput
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile

// Apache Imports
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.commons.io.FileUtils

// ScalaTest Imports
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

// Java Imports
import java.io.File


class QuestionOneTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  import com.nuvento.sparkexam.utils.SparkSetup.spark.implicits._

  val outputFilePath = "src/test/scala/com/nuvento/sparkexam/testoutput"

  before {
    // Create testoutput file with test data
    val testData = answer(outputFilePath)
    writeToFile(testData, outputFilePath)
  }

  after {
    // Delete testoutput directory and its contents
    FileUtils.deleteDirectory(new File("src/test/scala/com/nuvento/sparkexam/testoutput"))
  }

  test("Testing questionOne Program") {
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

  test("Testing answer function") {
    // Assert the structure of the processed data
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


    val result: Dataset[CustomerAccountOutput] = QuestionOne.answer(outputFilePath)
    assert(result.schema == expect)
  }
}