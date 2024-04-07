package com.nuvento.sparkexam

import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SetUpTest extends AnyFunSuite with BeforeAndAfter {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  test("Testing customerData via SetUp") {
    val result = SetUp.customerData.schema

    val expected = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true)
    ))

    assert(result === expected)
  }

  test("Testing accountData via SetUp") {
    val result = SetUp.accountData.schema

    val expected = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("accountId", StringType, nullable = true),
      StructField("balance", IntegerType, nullable = true)
    ))

    assert(result === expected)
  }

  test("Testing addressData via SetUp") {
    val result = SetUp.addressData.schema

    val expected = StructType(Seq(
      StructField("addressId", StringType, nullable = true),
      StructField("customerId", StringType, nullable = true),
      StructField("address", StringType, nullable = true)
    ))

    assert(result === expected)
  }
}
