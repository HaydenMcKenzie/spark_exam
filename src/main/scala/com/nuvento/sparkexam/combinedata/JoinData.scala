package com.nuvento.sparkexam.combinedata

import org.apache.spark.sql.Dataset

object JoinData extends App {
  """
    | @param firstData: Takes input Dataset
    | @param secondData: Takes input Dataset
    | @param column: String input for what column to join the Datasets
    |
    | @returns: A new Dataset that contains conjoined data from firstData and secondData
    |""".stripMargin

  def joinData(firstData: Dataset[_], secondData: Dataset[_], column: String): Dataset[_] = {
    firstData.join(secondData, column)
  }

}