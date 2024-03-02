package com.nuvento.sparkexam

import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.combinedata.JoinData.joinData
import com.nuvento.sparkexam.combinedata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile

import com.nuvento.sparkexam.SetUp.{customerData, accountData, parquetFilePath}

object QuestionOne extends App {
  // Spark setup
  SetUp.main(Array.empty[String])

  // Transform Data
  val joinDataByCustomerId = joinData(customerData, accountData, "customerId")
  val aggregated = aggregatedDataSet(joinDataByCustomerId, SparkSetup.spark)

  // Show and Write to file
  aggregated.show()

  writeToFile(aggregated, parquetFilePath)
}