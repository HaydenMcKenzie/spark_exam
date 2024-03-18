package com.nuvento.practice

import com.nuvento.sparkexam.comebinedata.TransformData.stringToSeq
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.utils.SparkSetup

object QuestionTwoPractice extends App {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._
  val parquetFilePath = "output"

  val importData = readFileData[Schemas.RawAddressSchema]("address_data")
  val transformData = importData.drop("addressId")

  val parquetDF = readParquetFile(parquetFilePath)
  val droppedParquet = parquetDF.drop("numberAccounts", "totalBalance", "averageBalance")

  val transformedData = droppedParquet.join(transformData, "customerId")
  val seqTransformedData = stringToSeq(transformedData, "address")

  seqTransformedData.show()

}
