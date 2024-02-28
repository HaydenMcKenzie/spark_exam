package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.combinedata.JoinData.joinData
import com.nuvento.sparkexam.combinedata.TransformData.aggregatedDataFrame
import com.nuvento.sparkexam.handlefiles.Schemas

object QuestionOne extends App {
  // Spark setup
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val customerData = readFileData[Schemas.customerSchemaTestRun]("customer_data")
  val accountData  = readFileData[Schemas.accountSchema]("account_data")

  // Transform Data
  val joinDS = joinData(customerData, accountData)
  val aggregatedDF = aggregatedDataFrame(joinDS, SparkSetup.spark)

  // Write to file - need help
  aggregatedDF.show()
  aggregatedDF.printSchema()

  try {
    val outputPath = "src/main/scala/com/nuvento/sparkexam/output"
    aggregatedDF.coalesce(1).write.parquet(outputPath)
  } catch {
    case e: Exception => println(s"File Already Exists")
  }
}