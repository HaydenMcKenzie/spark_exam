package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.combinedata.JoinData.joinData
import com.nuvento.sparkexam.combinedata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile

object QuestionOne extends App {
  // Spark setup
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // Input Dataset Data
  val customerData = readFileData[Schemas.customerSchema]("customer_data")
  val accountData  = readFileData[Schemas.accountSchema]("account_data")

  // Transform Data
  val joinDataByCustomerId = joinData(customerData, accountData, "customerId")
  val aggregated = aggregatedDataSet(joinDataByCustomerId, SparkSetup.spark)

  // Write to file
  aggregated.show()
  aggregated.printSchema()

  writeToFile(aggregated, "src/main/scala/com/nuvento/sparkexam/output")

}