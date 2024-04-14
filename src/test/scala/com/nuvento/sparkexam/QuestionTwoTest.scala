package com.nuvento.sparkexam

// Nuvento Imports
import com.nuvento.sparkexam.QuestionOne.answer
import com.nuvento.sparkexam.SetUp.addressData
import com.nuvento.sparkexam.handledata.Parsing.parseAddress
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.handlefiles.Schemas.{CustomerDocument, RawAddressData}
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile

// Apache Imports
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

// ScalaTest Imports
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

// Java Imports
import java.io.File


class QuestionTwoTest extends AnyFunSuite with BeforeAndAfter  {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val outputFilePath = "src/test/scala/com/nuvento/sparkexam/testoutput"
  val outputPath = "output"
  val addressString = "address"

  before {
    // Create testoutput file with test data
    val testData = answer(outputFilePath)
    val outputFile = answer("output")
    writeToFile(testData, outputFilePath)
    writeToFile(outputFile, outputPath)
  }

  after {
    // Delete testoutput directory and its contents
    FileUtils.deleteDirectory(new File(outputFilePath))
    FileUtils.deleteDirectory(new File(outputPath))
  }


  test("Testing questionTwo Schema") {
    val testParguetFile = readParquetFile("src/test/scala/com/nuvento/sparkexam/testoutput")
    val testParsedData = parseAddress(addressData, "address")
    val testMainProgram = QuestionTwo.questionTwo(testParguetFile, testParsedData)

    // Actual
    val actual = testMainProgram.schema

    // Except
    val expect = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(
        StructType(Seq(
          StructField("customerId", StringType, nullable = true),
          StructField("accountId", StringType, nullable = true),
          StructField("balance", IntegerType, nullable = true)
        )),
        containsNull = true
      ), nullable = true),
      StructField("address", ArrayType(
        StructType(Seq(
          StructField("addressId", StringType, nullable = true),
          StructField("customerId", StringType, nullable = true),
          StructField("address", StringType, nullable = true),
          StructField("number", IntegerType, nullable = true),
          StructField("road", StringType, nullable = true),
          StructField("city", StringType, nullable = true),
          StructField("country", StringType, nullable = true)
        )),
        containsNull = false
      ), nullable = false)
    ))

    // Test
    assert(actual == expect)
  }

  test("Testing answer function") {
    // Assert the structure of the processed data
    val expect = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(
        StructType(Seq(
          StructField("customerId", StringType, nullable = true),
          StructField("accountId", StringType, nullable = true),
          StructField("balance", IntegerType, nullable = true)
        )),
        containsNull = true
      ), nullable = true),
      StructField("address", ArrayType(
        StructType(Seq(
          StructField("addressId", StringType, nullable = true),
          StructField("customerId", StringType, nullable = true),
          StructField("address", StringType, nullable = true),
          StructField("number", IntegerType, nullable = true),
          StructField("road", StringType, nullable = true),
          StructField("city", StringType, nullable = true),
          StructField("country", StringType, nullable = true)
        )),
        containsNull = false
      ), nullable = false)
    ))


    val result: Dataset[CustomerDocument] = QuestionTwo.answer(outputFilePath, addressData, addressString)
    assert(result.schema == expect)
  }

  test("answer function should throw an exception when an error occurs") {
    // Create empty dataset to simulate test data
    val addressData: Dataset[RawAddressData] = null
    // Simulate invalid parquet file path
    val parquetFilePath: String = "invalid_path"

    // Call the answer method within a block and intercept the thrown exception
    val exception = intercept[Exception] {
      QuestionTwo.answer(parquetFilePath, addressData, "address")
    }

    // Print out the actual exception message
    println(s"Actual exception message: ${exception.getMessage}")

    // Assert that the exception is not null
    assert(exception != null)
    // Assert that the exception message contains a relevant error message
    assert(exception.getMessage.contains("Path does not exist") || exception.getMessage.contains("Error occurred"))
  }

  test("main method should execute without errors") {
    try {
      val outputStream = new java.io.ByteArrayOutputStream()
      Console.withOut(outputStream) {
        QuestionTwo.main(Array.empty[String])
      }

      assert(true)
    } catch {
      case e: Throwable =>
        fail(s"Exception occurred: ${e.getMessage}")
    }
  }
}
