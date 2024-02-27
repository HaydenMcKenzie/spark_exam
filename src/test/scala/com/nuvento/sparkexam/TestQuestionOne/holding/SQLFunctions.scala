package com.nuvento.sparkexam.TestQuestionOne.holding

import org.apache.spark.sql.DataFrame

object SQLFunctions {
  def selectColumnsFromData(inputDF: DataFrame): DataFrame = {
    inputDF.select("customerId", "forename", "surname")
  }

  def joinDataFrames(customerInfoDF: DataFrame, accountInfoDF: DataFrame, columnName: String): DataFrame = {
    customerInfoDF.join(accountInfoDF, columnName)
  }
}
