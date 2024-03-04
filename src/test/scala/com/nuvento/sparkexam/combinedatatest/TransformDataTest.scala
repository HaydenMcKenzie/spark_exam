package com.nuvento.sparkexam.combinedatatest

import com.nuvento.sparkexam.comebinedata.TransformData.{aggregatedDataSet, removeColumns, removeColumnsAndMergeTwoSetsOfData}
import com.nuvento.sparkexam.comebinedata.JoinData
import com.nuvento.sparkexam.handlefiles.ReadData.{readFileData, readParquetFile}
import com.nuvento.sparkexam.handlefiles.Schemas
import Schemas.customerSchema
import com.nuvento.sparkexam.utils.SparkSetup
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import com.nuvento.sparkexam.utilstest._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.Dataset

class TransformDataTest extends AnyFunSuite with BeforeAndAfter {
  SparkSetup.main(Array.empty[String])
  import com.nuvento.sparkexam.utils.SparkSetup.spark.implicits._

  // Local repeated data
  val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"

  val firstData: Dataset[_] = readFileData[Schemas.customerSchema]("customer_data")
  val secondData: Dataset[_] = readFileData[Schemas.accountSchema]("account_data")
  val thirdData: Dataset[_] = readFileData[Schemas.addressSchema]("address_data")
  val joiningDataForCount: Dataset[_] = JoinData.joinData(firstData, secondData, "customerId")

  val testParquetFilePath = "src/test/scala/com/nuvento/sparkexam/outputtest"

  // Testing aggrgatedDataSet function
  test("Test aggrgatedDataSet function with fixed data") {
    // Sample input data
    val firstData = Seq(
      ("1", "Alice", "Smith"),
      ("2", "Bob", "Munns"),
      ("3", "Charlie", "Johns"))
      .toDF("customerId", "forename", "surname")
    val secondData = Seq(
      ("1", "Acc1", 10),
      ("1", "Acc2", 10),
      ("2", "Acc3", 13),
      ("2", "Acc4", 10),
      ("2", "Acc5", 10),
      ("3", "Acc6", 15))
      .toDF("customerId", "accountId", "balance")
    val joiningFixedData = JoinData.joinData(firstData, secondData, "customerId")

    // Call the function
    val result: Dataset[_] = aggregatedDataSet(joiningFixedData)

    // Expected result
    val expected = Seq(
      ("3", "Charlie", "Johns", Array("Acc6"),1, 15, 15.0),
      ("1", "Alice", "Smith", Array("Acc2", "Acc1"), 2, 20, 10.0),
      ("2", "Bob", "Munns", Array("Acc3", "Acc5", "Acc4"), 3, 33, 11.0))
      .toDF("customerId", "forename", "surname", "accounts", "numberAccounts", "totalBalance", "averageBalance")

    // Compare the result with the expected output
    assert(result.collect().sameElements(expected.collect()))
  }

  test("Test aggrgatedDataSet function is equal to 339") {
    // Call the function
    val result: Dataset[_] = aggregatedDataSet(joiningDataForCount)

    // Test if it is has more than 0
    assert(result.count() == 339)
  }

  test("Test aggrgatedDataSet function Schema") {
    // Call the function
    val result: Dataset[_] = aggregatedDataSet(joiningDataForCount)
    val actualSchema = result.schema

    // Expected
    val expectedSchema = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(StringType, false), nullable = true),
      StructField("numberAccounts", IntegerType, nullable = false),
      StructField("totalBalance", LongType, nullable = true),
      StructField("averageBalance", DoubleType, nullable = true)
    ))

    // Compare the result with the expected output
    assert(actualSchema == expectedSchema)
  }

  // Testing removeColumns function
  test("Test removeColumns function with one column input") {
    // Sample input data
    val testingData: Dataset[customerSchema] = readFileData[Schemas.customerSchema]("customer_data")

    // Call the function
    val result: Dataset[_] = removeColumns(testingData, "customerId")
    val resultSchema = result.schema

    // Expected
    val expected = StructType(Seq(
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
    ))

    // Test
    assert(resultSchema == expected)
  }

  test("Test removeColumns function with multiple column input") {
    // Sample input data
    val testingData: Dataset[customerSchema] = readFileData[Schemas.customerSchema]("customer_data")

    // Call the function
    val result: Dataset[_] = removeColumns(testingData, "customerId, surname")
    val resultSchema = result.schema

    // Expected
    val expected = StructType(Seq(
      StructField("forename", StringType, nullable = true),
    ))

    // Test
    assert(resultSchema == expected)
  }

  // Testing stringtoSeq function
  test("Test stringtoSeq function turns address column from a string to an array") {
    // Sample input data
    val testingData = removeColumnsAndMergeTwoSetsOfData(readParquetFile(testParquetFilePath), "numberAccounts, totalBalance, averageBalance", thirdData, "addressId")

    // Call the function
    val result: Dataset[_] = testingData.select("address")

    // Expected
    val expectedSchema = StructType(Seq(StructField("address", ArrayType(StringType, true), nullable = true)))

    // Test if it is has more than 0
    assert(result.schema == expectedSchema)
  }
}
