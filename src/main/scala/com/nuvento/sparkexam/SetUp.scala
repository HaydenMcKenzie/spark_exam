package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData.readFileData
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.utils.SparkSetup

object SetUp extends App {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val customerData = readFileData[Schemas.customerSchema]("customer_data")
  val accountData = readFileData[Schemas.accountSchema]("account_data")
  val addressData = readFileData[Schemas.addressSchema]("address_data")

  val parquetFilePath = "output"
}
