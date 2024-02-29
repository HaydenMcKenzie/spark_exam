package com.nuvento.sparkexam.TestQuestionOne.combinedata

import com.nuvento.sparkexam.combinedata.JoinData
import com.nuvento.sparkexam.combinedata.TransformData.{aggregatedDataSet, removeColumnsAndMergeIntoOneTable, stringToSeq}
import com.nuvento.sparkexam.handlefiles.ReadData.{readFileData, readParquetFile}
import com.nuvento.sparkexam.handlefiles.Schemas
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import com.nuvento.sparkexam.utils._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class TransformDataTest extends AnyFunSuite with BeforeAndAfter {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // Testing aggrgatedDataSet function
  test("Test aggrgatedDataSet function with fixed data") {
    // Sample input data
    val firstData = Seq(
      ("1", "Alice", "Smith"),
      ("2", "Bob", "Munns"),
      ("3", "Charlie", "Johns"))
      .toDF("customerId", "forename", "surname")
    val secondData = Seq(
      ("1", "Acc1", "10"),
      ("1", "Acc2", "10"),
      ("2", "Acc3", "13"),
      ("2", "Acc4", "10"),
      ("2", "Acc5", "10"),
      ("3", "Acc6", "15"))
      .toDF("customerId", "accountId", "balance")
    val joiningFixedData = JoinData.joinData(firstData, secondData)

    // Call the function
    val result: Dataset[_] = aggregatedDataSet(joiningFixedData, SparkSetup.spark)

    // Expected result
    val expected = Seq(
      ("3", "Charlie", "Johns", Array("Acc6"),1, "$15.00", "$15.00"),
      ("1", "Alice", "Smith", Array("Acc2", "Acc1"), 2, "$20.00", "$10.00"),
      ("2", "Bob", "Munns", Array("Acc3", "Acc5", "Acc4"), 3, "$33.00", "$11.00"))
      .toDF("customerId", "forename", "surname", "accounts", "numberAccounts", "totalBalance", "averageBalance")

    // Compare the result with the expected output
    assert(result.collect().sameElements(expected.collect()))
  }

  test("Test aggrgatedDataSet function Count more than 0") {
    // Sample input data
    val firstData = readFileData[Schemas.customerSchema]("customer_data")
    val secondData = readFileData[Schemas.accountSchema]("account_data")
    val joiningDataForCount = JoinData.joinData(firstData, secondData)

    // Call the function
    val result: Dataset[_] = aggregatedDataSet(joiningDataForCount, SparkSetup.spark)

    // Test if it is has more than 0
    assert(result.count() > 0)
  }

  test("Test aggrgatedDataSet function is equal to 339") {
    // Sample input data
    val firstData = readFileData[Schemas.customerSchema]("customer_data")
    val secondData = readFileData[Schemas.accountSchema]("account_data")
    val joiningDataForCount = JoinData.joinData(firstData, secondData)

    // Call the function
    val result: Dataset[_] = aggregatedDataSet(joiningDataForCount, SparkSetup.spark)

    // Test if it is has more than 0
    assert(result.count() == 339)
  }

  test("Test aggrgatedDataSet function Schema") {
    // Input data
    val firstData = readFileData[Schemas.customerSchema]("customer_data")
    val secondData = readFileData[Schemas.accountSchema]("account_data")
    val joiningDataForCount = JoinData.joinData(firstData, secondData)

    // Call the function
    val joinedData: Dataset[_] = aggregatedDataSet(joiningDataForCount, SparkSetup.spark)
    val actualSchema = joinedData.schema

    // Expected
    val expectedSchema = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(StringType, false), nullable = true),
      StructField("numberAccounts", LongType, nullable = false),
      StructField("totalBalance", StringType, nullable = true),
      StructField("averageBalance", StringType, nullable = true)
    ))

    // Compare the result with the expected output
    assert(actualSchema == expectedSchema)
  }

  // Testing removeColumns function
  test("Test removeColumns function with actual data") {
    // Sample input data
    val TestingData = removeColumnsAndMergeIntoOneTable(readFileData[Schemas.addressSchema]("address_data"), readParquetFile())

    // Call the function
    val result = TestingData.schema

    // Expected
    val expectedSchema = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(StringType, true), nullable = true),
      StructField("address", StringType, nullable = true),
    ))

    // Test if it is has more than 0
    assert(result == expectedSchema)
  }

  test("Test removeColumns function is exactly 339") {
    // Sample input data
    val TestingData = removeColumnsAndMergeIntoOneTable(readFileData[Schemas.addressSchema]("address_data"), readParquetFile())

    // Call the function
    val result = TestingData.count()

    // Test if it is has more than 0
    assert(result == 339)
  }

  // Testing stringtoSeq function
  test("Test stringtoSeq function turns address column from a string to an array") {
    // Sample input data
    val TestingData = removeColumnsAndMergeIntoOneTable(readFileData[Schemas.addressSchema]("address_data"), readParquetFile())

    // Call the function
    val result: Dataset[_] = stringToSeq(TestingData).select("address")

    // Expected
    val expectedSchema = StructType(Seq(StructField("address", ArrayType(StringType, true), nullable = true)))

    // Test if it is has more than 0
    assert(result.schema == expectedSchema)
  }

}
