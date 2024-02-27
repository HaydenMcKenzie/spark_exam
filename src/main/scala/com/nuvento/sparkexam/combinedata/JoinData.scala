package com.nuvento.sparkexam.combinedata

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinData extends App {
  """
    | @param customerData: Dataset from ReadData
    | @param accountData: Dataset from ReadData
    | @returns Dataframe
    |
    | Joins accountData to customerData via the "customerId" column
    |""".stripMargin

  def joinDataSetsToDataFrame(customerData: Dataset[_], accountData: Dataset[_]): DataFrame = {
    customerData.join(accountData, "customerId")
  }

}