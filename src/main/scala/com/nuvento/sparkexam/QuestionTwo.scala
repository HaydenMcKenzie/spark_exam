package com.nuvento.sparkexam

import com.nuvento.sparkexam.comebinedata.TransformData.{removeColumns, stringToSeq}
import com.nuvento.sparkexam.comebinedata.JoinData
import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.AddColumn.grabColumn
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])

  try {
    // Transforming the Data
    val parquetFile = readParquetFile("output")

    val removedColumnsForFirstData = removeColumns(parquetFile, "numberAccounts, totalBalance, averageBalance")
    val removedColumnsForSecondFile = removeColumns(addressData, "addressId")
  removedColumnsForSecondFile.show()

    val joinedColumnsThatHaventBeenRemoved = JoinData.joinData(removedColumnsForFirstData, removedColumnsForSecondFile, "customerId", "left")
    val removedColumnsForThirdFile = removeColumns(joinedColumnsThatHaventBeenRemoved, "address")
  removedColumnsForThirdFile.show()

    val processData = grabColumn(addressData, "address").select( "customerId","address")
  processData.show()
    val test = JoinData.simpleJoinData(removedColumnsForThirdFile,processData,"customerId")
    // Printing
    test.show()
  } catch {
    case e: Exception => println(s"File Does Not Exists.")
  }
}