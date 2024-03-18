package com.nuvento.sparkexam.combinedata

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.{readFileData, readParquetFile}
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ParsingTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  test("Testing parseAddress") {
    // Import
    val addressData = readFileData[Schemas.RawAddressSchema]("address_data")
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
    val parquetFile = readParquetFile(parquetFilePath)
    val parsedData = parseAddress(addressData, "address")
    val joinData = parquetFile.join(parsedData, "customerId")
    val processData = createCustomerDocument(joinData)

    // Result
    val result = processData.schema

    // Expeceted
    val expected = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accounts", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("address", ArrayType(StructType(Seq(
        StructField("addressId", StringType, nullable = true),
        StructField("customerId", StringType, nullable = true),
        StructField("address", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true),
        StructField("road", StringType, nullable = true),
        StructField("city", StringType, nullable = true),
        StructField("country", StringType, nullable = true)
      )), containsNull = false), nullable = true)
    ))

    // Test
    assert(result == expected)
  }
}
