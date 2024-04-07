package com.nuvento.sparkexam

import com.nuvento.sparkexam.handledata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile
import com.nuvento.sparkexam.SetUp.{accountData, customerData, parquetFilePath}
import com.nuvento.sparkexam.handlefiles.Schemas.CustomerAccountOutput
import org.apache.spark.sql.{Dataset,Encoders}
import com.nuvento.sparkexam.handlefiles.Schemas._
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.functions._


object QuestionOne extends App {
  // Spark setup
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // Transform Data
  def questionOne(customerData: Dataset[RawCustomerSchema], accountData: Dataset[RawAccountSchema]): Dataset[CustomerAccountOutput] = {
    """
      | Main Program for Question One
      |
      | @param customerData: A Dataset from customer_data.csv with the schema of RawCustomerSchema
      | @param accountData: A Dataset from account_data.csv with the schema of RawAccountSchema
      |
      | @function aggregated:
      | Joins customerData and accountData via customerId. This is by a left join
      | It then created accounts, totalBalance and averageBalance
      |
      | @return: a new Dataset with the schema of CustomerAccountOutput and writes it to file
      |""".stripMargin

    val aggregated: Dataset[CustomerAccountOutput] = aggregatedDataSet(customerData, accountData)(Encoders.product[CustomerAccountOutput])

    aggregated
  }

  // Show and Write to file
  val answer: Dataset[CustomerAccountOutput] = questionOne(customerData,accountData)
  answer.show(false)

  writeToFile(answer, parquetFilePath) // compression
}