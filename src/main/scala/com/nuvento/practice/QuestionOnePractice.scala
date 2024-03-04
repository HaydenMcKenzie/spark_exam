package com.nuvento.practice

import com.nuvento.sparkexam.comebinedata.JoinData
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.utils.SparkSetup

object QuestionOnePractice extends App {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._
  val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"

  val importData = readFileData[Schemas.addressSchema]("address_data")
  val transformData = importData.drop("addressId")

  val parquetDF = readParquetFile("src/main/scala/com/nuvento/sparkexam/output")
  val droppedParquet = parquetDF.drop("numberAccounts", "totalBalance", "averageBalance")

  val processData = JoinData.joinData(droppedParquet, transformData, "customerId")

  processData.show()
}
