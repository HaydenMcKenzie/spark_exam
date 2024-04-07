package com.nuvento.sparkexam.handledata

import com.nuvento.sparkexam.handledata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.handlefiles.ReadData.readFileData
import com.nuvento.sparkexam.handlefiles.Schemas
import Schemas.CustomerAccountOutput
import com.nuvento.sparkexam.utils.SparkSetup
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoders, Row}

class TransformDataTest extends AnyFunSuite with BeforeAndAfter {
  SparkSetup.main(Array.empty[String])
  import com.nuvento.sparkexam.utils.SparkSetup.spark.implicits._

  // Local repeated data
  val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"
  val testParquetFilePath = "src/test/scala/com/nuvento/sparkexam/outputtest"

  val customerData: Dataset[_] = readFileData[Schemas.RawCustomerSchema]("customer_data")
  val accountData: Dataset[_] = readFileData[Schemas.RawAccountSchema]("account_data")
  val addressData: Dataset[_] = readFileData[Schemas.RawAddressSchema]("address_data")
  val joiningDataForCount: Dataset[_] = customerData.join(accountData, Seq("customerId"), "left")



  test("Test aggrgatedDataSet function is equal to 500") {
    // Call the function
    val result: Dataset[CustomerAccountOutput] = aggregatedDataSet(customerData, accountData)(Encoders.product[CustomerAccountOutput])

    // Test if it is has more than 0
    assert(result.count() == 500)
  }

  test("Test aggregatedDataSet function Schema") {
    // Call the function
    val result: Dataset[CustomerAccountOutput] = aggregatedDataSet(customerData, accountData)(Encoders.product[CustomerAccountOutput])
    val actualSchema = result.schema

    // Expected
    val expectedSchema = StructType(Array(
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

    // Compare the result with the expected output
    assert(actualSchema == expectedSchema)
  }

  test("Test If Customer With No Accounts Seq Is Empty") {
    // Call the function
    val testData: Dataset[CustomerAccountOutput] = aggregatedDataSet(customerData, accountData)(Encoders.product[CustomerAccountOutput])

    // Expected
    val filterData = testData
      .filter($"customerId" === "IND0277")
      .select($"accounts")
    val expected = filterData.collect()

    // Compare the result with the expected output
    val accountsArray = expected.head.getAs[Seq[(String, String, Int)]](0)
    assert(accountsArray.isEmpty)
  }

  test("Test If Customer With Accounts Seq Is Non-Empty") {
    // Call the function
    val testData: Dataset[CustomerAccountOutput] = aggregatedDataSet(customerData, accountData)(Encoders.product[CustomerAccountOutput])

    // Expected
    val filterData = testData
      .filter($"customerId" === "IND0113")
      .select($"accounts")
    val expected = filterData.collect()

    // Compare the result with the expected output
    val accountsArray = expected.head.getAs[Seq[(String, String, Int)]](0)
    assert(accountsArray.nonEmpty)
  }
}
