package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData.readFileData
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.utils.SparkSetup

object SetUp extends App {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  lazy val customerData = readFileData[Schemas.RawCustomerSchema]("customer_data")
  lazy val accountData = readFileData[Schemas.RawAccountSchema]("account_data")
  lazy val addressData = readFileData[Schemas.RawAddressSchema]("address_data")

  val parquetFilePath = "output"
}
