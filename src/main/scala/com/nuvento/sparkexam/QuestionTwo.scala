package com.nuvento.sparkexam

import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.combinedata.TransformData.removeColumnsAndMergeTwoSetsOfData

object QuestionTwo extends App {
  // Spark Setup
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // file path
  val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"

  // Transforming the Data
  val processData = removeColumnsAndMergeTwoSetsOfData(readParquetFile(parquetFilePath), "numberAccounts, totalBalance, averageBalance", readFileData[Schemas.addressSchema]("address_data"), "addressId")

  // Printing
  processData.show()
  processData.printSchema()
}
