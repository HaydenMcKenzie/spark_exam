package com.nuvento.sparkexam.handledata

// Nuvento Imports
import com.nuvento.sparkexam.QuestionOne.answer
import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handledata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.{readFileData, readParquetFile}
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile

// Java Imports
import java.io.File

// Apache Imports
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.commons.io.FileUtils

// ScalaTest Imports
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter


class ParsingTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

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

  test("Testing parseAddress") {
    // Import
    val addressData = readFileData[Schemas.RawAddressData]("address_data")
    val parsedDataTest = parseAddress(addressData, "address")

    // Result
    val result = parsedDataTest.schema

    // Expeceted
    val expected = StructType(Seq(
      StructField("addressId", StringType, nullable = true),
      StructField("customerId", StringType, nullable = true),
      StructField("address", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true),
      StructField("road", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("country", StringType, nullable = true)
    ))

    // Test
    assert(result == expected)
  }


  test("Testing createCustomerDocument") {
    // Import
    val importAddressData = SetUp.addressData

    val parquetFile = readParquetFile(outputFilePath)
    val parsedData = parseAddress(importAddressData, "address")
    val joinData = parquetFile.join(parsedData, "customerId")
    val processData = createCustomerDocument(joinData)

    // Result
    val result = processData.schema

    // Expeceted
    val expected = StructType(Seq(
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
    assert(result == expected)
  }
}
