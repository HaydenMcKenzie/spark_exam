package com.nuvento.practice

import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.combinedata.JoinData._
import com.nuvento.sparkexam.combinedata.TransformData.removeColumnsAndMergeIntoOneTable

object play extends App {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val importData = readFileData[Schemas.addressSchema]("address_data")
  val transformData = importData.drop("addressId")

  val parquetDF = readParquetFile()
  val droppedParquet = parquetDF.drop("numberAccounts", "totalBalance", "averageBalance")

  val removedColumns = removeColumnsAndMergeIntoOneTable(transformData, droppedParquet)

  removedColumns.show()
}
