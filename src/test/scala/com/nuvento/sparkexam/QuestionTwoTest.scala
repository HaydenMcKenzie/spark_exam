package com.nuvento.sparkexam

import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.handledata.Parsing.parseAddress
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._

class QuestionTwoTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])

  test("Testing Question Two Main Program") {
    val testParguetFile = readParquetFile(parquetFilePath)
    val testParsedData = parseAddress(addressData, "address")
    val testMainProgram = QuestionTwo.questionTwo(testParguetFile, testParsedData)

    // Actual
    val actual = testMainProgram.schema

    // Except
    val expect = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(StringType, containsNull = true), nullable = true),
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
      ), nullable = true)
    ))

    // Test
    assert(actual == expect)
  }
}
