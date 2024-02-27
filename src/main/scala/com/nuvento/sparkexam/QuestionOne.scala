package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.combinedata.JoinData.joinDataSetsToDataFrame
import com.nuvento.sparkexam.combinedata.TransformData.aggregatedDataFrame
import com.nuvento.sparkexam.handlefiles.schemas

object QuestionOne extends App {
  // Spark setup
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val customerData = readFileData[schemas.customerSchemaTestRun]("customer_data")
  val accountData  = readFileData[schemas.accountSchema]("account_data")

  // Transform Data
  val joinDS = joinDataSetsToDataFrame(customerData, accountData)
  val aggregatedDF = aggregatedDataFrame(joinDS, SparkSetup.spark)

  // Write to file - need help
  aggregatedDF.show()

  val outputPath = "src/main/scala/com/nuvento/sparkexam/output"
  aggregatedDF.coalesce(1).write.parquet(outputPath)
}