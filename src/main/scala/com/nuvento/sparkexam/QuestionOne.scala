package com.nuvento.sparkexam

import com.nuvento.sparkexam.handledata.TransformData.aggregatedDataSet
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile
import com.nuvento.sparkexam.SetUp.{accountData, customerData}
import com.nuvento.sparkexam.handlefiles.Schemas.CustomerAccountOutput
import org.apache.spark.sql.{Dataset,Encoders}
import com.nuvento.sparkexam.handlefiles.Schemas._
import com.nuvento.sparkexam.utils.SparkSetup

object QuestionOne {
  // Spark setup
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  def questionOne(customerData: Dataset[RawCustomerData], accountData: Dataset[RawAccountData]): Dataset[CustomerAccountOutput] = {
    """
      | Main Program for Question One
      |
      | @param customerData: A Dataset from customer_data.csv with the schema of RawCustomerSchema
      | @param accountData: A Dataset from account_data.csv with the schema of RawAccountSchema
      |
      | @function aggregated:
      | Joins customerData and accountData via customerId. This is by a left join
      | It then creates accounts, totalBalance and averageBalance columns
      |
      | @return Dataset[CustomerAccountOutput]: a new Dataset with the schema of CustomerAccountOutput
      |""".stripMargin

    val aggregated: Dataset[CustomerAccountOutput] = aggregatedDataSet(customerData, accountData)(Encoders.product[CustomerAccountOutput])

    aggregated
  }

  // Show and Write to file
  def answer(outputPath: String): Dataset[CustomerAccountOutput] = {
    """
      | answer Program for Question One
      |
      | @param outputPath: output pathway for writeToFile to send output file
      |
      | @function answer:
      | Pass questionOne with customerData and accountData
      | Show answer
      | Write to parquet file
      |
      | @return Dataset[CustomerAccountOutput]: a new Dataset with the schema of CustomerAccountOutput and writes it to file
      |""".stripMargin

    val answer: Dataset[CustomerAccountOutput] = questionOne(customerData, accountData)
    answer.show(false)

    writeToFile(answer, outputPath)
    answer
  }


  def main(args: Array[String]): Unit  = {
    // Call App
    answer("output")
  }
}