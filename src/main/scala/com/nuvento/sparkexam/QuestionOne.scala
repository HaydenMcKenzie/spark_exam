package com.nuvento.sparkexam

import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.combinedata.JoinData.joinData
import com.nuvento.sparkexam.combinedata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.handlefiles.Schemas

object QuestionOne extends App {
  // Spark setup
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  val customerData = readFileData[Schemas.customerSchema]("customer_data")
  val accountData  = readFileData[Schemas.accountSchema]("account_data")

  // Transform Data
  val joinDS = joinData(customerData, accountData)
  val aggregatedDF = aggregatedDataSet(joinDS, SparkSetup.spark)

  // Write to file
  aggregatedDF.show()
  aggregatedDF.printSchema()

  try {
    val outputPath = "src/main/scala/com/nuvento/sparkexam/output"
    aggregatedDF.coalesce(1).write.parquet(outputPath)
    println("File has been created")
  } catch {
    case e: Exception => println(s"File Already Exists")
  }
}