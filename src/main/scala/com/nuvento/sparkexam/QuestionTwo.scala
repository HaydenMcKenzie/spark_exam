package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.comebinedata.TransformData.removeColumnsAndMergeTwoSetsOfData
import com.nuvento.sparkexam.SetUp.{parquetFilePath, addressData}

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])

  // Transforming the Data
  val processData = removeColumnsAndMergeTwoSetsOfData(readParquetFile(parquetFilePath), "numberAccounts, totalBalance, averageBalance", addressData, "addressId")

  // Printing
  processData.show()
}