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

  def joinData(firstData: Dataset[_], secondData: Dataset[_]): Dataset[_] = {
    firstData.join(secondData, "customerId")
  }

}