package com.nuvento.practice

import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.schemas

object play extends App {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val importData = readFileData[schemas.addressSchema]("address_data")
  val transformData = importData.drop("addressId")

  val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"
  val parquetDF = SparkSetup.spark.read.parquet(parquetFilePath)
  val droppedParquet = parquetDF.drop("numberAccounts", "totalBalance", "averageBalance")

  droppedParquet.show()
}
