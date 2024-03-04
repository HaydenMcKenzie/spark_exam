package com.nuvento.sparkexam.comebinedata

import org.apache.spark.sql.Dataset

object JoinData extends App {
  def joinData(firstDataSet: Dataset[_], secondDataSet: Dataset[_], column: String): Dataset[_] = {
    """
      | @param firstData: Takes input Dataset that will be base Dataset
      | @param secondData: Takes input Dataset that will be joined to the base Dataset
      | @param column: String input for what column to join the Datasets
      |
      | @returns: A new Dataset that contains conjoined data from firstDataSet and secondDataSet
      |""".stripMargin

    firstDataSet.join(secondDataSet, column)
  }

}