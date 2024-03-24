package com.nuvento.sparkexam

import com.nuvento.sparkexam.handledata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile
import com.nuvento.sparkexam.SetUp.{accountData, customerData, parquetFilePath}
import org.apache.spark.sql.Dataset

object QuestionOne extends App {
  // Spark setup
  SetUp.main(Array.empty[String])

  // Transform Data
  def questionOne(): Dataset[_] = {
    """
      | Main Program for Question One
      |
      | Joins customer_data.csv and account_data.csv via customerData and accountData. This is by a left join
      | It then created accounts, totalBalance and averageBalance
      | Returns a new Dataset to be written as a parquet file
      |""".stripMargin

    val joinDataByCustomerId = customerData.join(accountData, Seq("customerId"), "left")
    val aggregated = aggregatedDataSet(joinDataByCustomerId)

    aggregated
  }

  // Show and Write to file
  val answer = questionOne()
  answer.show(false)

  writeToFile(answer, parquetFilePath)
}