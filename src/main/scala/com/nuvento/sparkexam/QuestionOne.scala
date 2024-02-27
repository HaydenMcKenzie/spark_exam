package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.combinedata.JoinData.joinDataSetsToDataFrame
import com.nuvento.sparkexam.combinedata.TransformData.aggregatedDataFrame

object QuestionOne extends App {
  // Spark setup
  SparkSetup.main(Array.empty[String])

  val customerData = readCustomerData()
  val accountData  = readAccountData()

  // Transform Data
  val joinDS = joinDataSetsToDataFrame(customerData, accountData)
  val aggregatedDF = aggregatedDataFrame(joinDS, SparkSetup.spark)

  // Write to file - need help
  aggregatedDF.show()
}