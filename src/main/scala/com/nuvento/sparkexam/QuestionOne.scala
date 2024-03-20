package com.nuvento.sparkexam

import com.nuvento.sparkexam.comebinedata.JoinData.joinData
import com.nuvento.sparkexam.comebinedata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile
import com.nuvento.sparkexam.SetUp.{accountData, customerData, parquetFilePath}
import org.apache.spark.sql.Dataset

object QuestionOne extends App {
  // Spark setup
  SetUp.main(Array.empty[String])

  // Transform Data
  def questionOne(customerDataInput: Dataset[_], accountDataInput: Dataset[_], column: String, joinType: String): Dataset[_] = {
    val joinDataByCustomerId = joinData(customerDataInput, accountDataInput, column, joinType)
    val aggregated = aggregatedDataSet(joinDataByCustomerId)

    aggregated
  }

  // Show and Write to file
  val answer = questionOne(customerData, accountData, "customerId", "left")
  answer.show(false)

  writeToFile(answer, parquetFilePath)
}