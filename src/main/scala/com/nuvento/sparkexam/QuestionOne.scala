package com.nuvento.sparkexam

import com.nuvento.sparkexam.comebinedata.JoinData.joinData
import com.nuvento.sparkexam.comebinedata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile

import com.nuvento.sparkexam.SetUp.{customerData, accountData, parquetFilePath}

object QuestionOne extends App {
  // Spark setup
  SetUp.main(Array.empty[String])

  // Transform Data
  val joinDataByCustomerId = joinData(customerData, accountData, "customerId", "left")
  val aggregated = aggregatedDataSet(joinDataByCustomerId)

  // Show and Write to file
  aggregated.show()

  writeToFile(aggregated, parquetFilePath)
}