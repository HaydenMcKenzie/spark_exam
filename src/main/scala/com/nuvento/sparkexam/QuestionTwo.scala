package com.nuvento.sparkexam

import com.nuvento.sparkexam.comebinedata.TransformData.{removeColumns, stringToSeq}
import com.nuvento.sparkexam.comebinedata.JoinData
import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])

  // Transforming the Data
  val parquetFile = readParquetFile(parquetFilePath)

  val removedColumnsForFirstData = removeColumns(parquetFile, "numberAccounts, totalBalance, averageBalance")
  val removedColumnsForSecondFile = removeColumns(addressData, "addressId")

  val joinedColumnsThatHaventBeenRemoved = JoinData.joinData(removedColumnsForFirstData, removedColumnsForSecondFile, "customerId")
  val processData = stringToSeq(joinedColumnsThatHaventBeenRemoved, "address")

  // Printing
  processData.show()
}