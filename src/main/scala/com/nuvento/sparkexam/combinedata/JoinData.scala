package com.nuvento.sparkexam.combinedata

import org.apache.spark.sql.Dataset

object JoinData extends App {
  """
    | @param customerData: Dataset from ReadData
    | @param accountData: Dataset from ReadData
    | @returns: A new Dataset that contains conjoined data from customerData and accountData
    |
    | Joins accountData to customerData via the "customerId" column
    |""".stripMargin

  def joinData(firstData: Dataset[_], secondData: Dataset[_], column: String): Dataset[_] = {
    firstData.join(secondData, column)
  }

}