package com.nuvento.practice

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.utils._
import org.apache.spark.sql.functions.{collect_list, struct}

object play extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val parsedDataTest = parseAddress(addressData, "address")
  parsedDataTest.show()
  parsedDataTest.printSchema()



  case class AddressData(addressId: String, customerId: String, address: String, number: Option[Int], road: Option[String], city: Option[String], country: Option[String])

  case class CustomerDocument(customerId: String, forename: String, surname: String, accounts: Seq[String], address: Seq[AddressData])


  val parquetFile = readParquetFile(parquetFilePath).drop("numberAccounts","totalBalance","averageBalance")
  parquetFile.show()
  val parsedData = parseAddress(addressData, "address").as[AddressData]
  parsedData.show()

  val addressStruct = struct(
    $"addressId",
    $"customerId",
    $"address",
    $"number",
    $"road",
    $"city",
    $"country"
  )

  val joinX = parquetFile.join(parsedData, "customerId")
  val joinD = parquetFile.join(parsedData, "customerId")
    .groupBy($"customerId", $"forename", $"surname", $"accounts")
    .agg(collect_list(addressStruct).as("address"))
    .select(
      $"customerId",
      $"forename",
      $"surname",
      $"accounts",
      $"address".as[Seq[AddressData]],
    )
  joinD.show(false)

  createCustomerDocument(joinX).show(false)
}
