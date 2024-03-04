package com.nuvento.sparkexam.comebinedata

import com.nuvento.sparkexam.SetUp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType}

object TransformData extends App {
  SetUp.main(Array.empty[String])

  // Question 1 Functions

  def aggregatedDataSet(data: Dataset[_]): Dataset[_] = {
    """
      | @param joinedData: Joined Data from joinData() in question 1
      |
      | @return: Returns new Dataset adding accounts, numberAccounts, totalBalance and averageBalance columns
      |
      | Groups by "customerId", "forename", "surname"
      | Collects all accountIds and puts them into a Seq. Renames column to accounts
      | Counts call elements in Seq. Renames column to numberAccounts
      | Adds all accounts balances to 2 decimal places. Renames column to totalBalance and puts $ at the front
      | Averages all accounts balances to 2 decimal places. Renames column to averageBalance and puts $ at the front
      |""".stripMargin

    data.groupBy("customerId", "forename", "surname")
      .agg(
        collect_list("accountId").alias("accounts"),
        countDistinct("accountId").cast(IntegerType).alias("numberAccounts"),
        sum("balance").cast(LongType).alias("totalBalance"),  // does it need to be rounded?
        round(avg("balance"), 2).alias("averageBalance")
      )
  }

  // Question 2 Functions

  def removeColumns(data: Dataset[_], stringInput: String): Dataset[_] = {
    """
      | @param data: Takes Data file as a Dataset
      | @param stringInput: Input String for columns that need to removed
      |
      | @return: New Dataset with remaining columns
      |""".stripMargin

    // Split input strings by comma and trim whitespace
    val firstColumns = stringInput.split(",").map(_.trim)

    // Select only the columns you want to keep
    val selectedColumns = data.columns.filterNot(firstColumns.contains)
    val transformedData = data.select(selectedColumns.head, selectedColumns.tail: _*)

    transformedData
  }

  def stringToSeq(data: Dataset[_], column: String): Dataset[_] = {
    """
      | @param data: Takes Data file as a Dataset
      | @param column: Takes a String input for column that needs to be changed to an ArrayType(StringType)
      | @returns: Column is switched from a StringType to ArrayType(StringType)
      |""".stripMargin

    data.withColumn(column, split(col(column), ",").cast(ArrayType(StringType)))
      .withColumn(column, expr("transform("+column+", x -> trim(x))"))
  }
}
