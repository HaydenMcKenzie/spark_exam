package com.nuvento.sparkexam

import com.nuvento.sparkexam.comebinedata.TransformData.{removeColumns, stringToSeq}
import com.nuvento.sparkexam.comebinedata.JoinData
import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import .grabColumn
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])

  try {
    // Transforming the Data
    val parquetFile = readParquetFile(parquetFilePath)

    // Remove columns from existing files
    val removedColumnsForFirstData = removeColumns(parquetFile, "numberAccounts, totalBalance, averageBalance")
    val removedColumnsForSecondFile = removeColumns(addressData, "addressId")

    // Join remaining columns and Remove address column
    val joinedColumnsThatHaventBeenRemoved = JoinData.joinData(removedColumnsForFirstData, removedColumnsForSecondFile, "customerId", "left")
    val removedColumnsForThirdFile = removeColumns(joinedColumnsThatHaventBeenRemoved, "address")

    // Turn addressData into a line of strings and made it the address column. Select customerId and address columns
    //val processData = grabColumn(addressData, "address").select( "customerId","address")

    // Join address with remain columns. Turn Address into Seq
    //val test = JoinData.simpleJoinData(removedColumnsForThirdFile,processData,"customerId")

    // Print
    //test.show(1000,truncate = false)
    //test.select("address").show(false)
    //test.printSchema()

  } catch {
    case e: Exception => println(s"File Does Not Exists.")
  }
}