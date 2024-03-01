package com.nuvento.sparkexam.combinedata

import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType}

object TransformData extends App {
  def aggregatedDataSet(joinedDF: Dataset[_], spark: SparkSession): Dataset[_] = {
    """
      | @param joinedDF: Dataframe from JoinData
      | @param spark: Access SparkSession for certain features needed
      | @return: Returns new Dataset adding accounts, numberAccounts, totalBalance and averageBalance
      |
      | concatAcccuntUDF creates a template Seq for accounts
      |
      | Groups by "customerId", "forename", "surname"
      | Collects all accountIds and puts them into a Seq. Renames column to accounts
      | Counts call elements in Seq. Renames column to numberAccounts
      | Adds all accounts balances to 2 decimal places. Renames column to totalBalance and puts $ at the front
      | Averages all accounts balances to 2 decimal places. Renames column to averageBalance and puts $ at the front
      |""".stripMargin
    import spark.implicits._

    joinedDF.groupBy("customerId", "forename", "surname")
      .agg(
        collect_list("accountId").alias("accounts"),
        countDistinct("accountId").cast(IntegerType).alias("numberAccounts"),
        sum("balance").cast(LongType).alias("totalBalance"),  // does it need to be rounded?
        round(avg("balance"), 2).alias("averageBalance")
      )
  }

  def removeColumns(firstData: Dataset[_], firstInput: String): Dataset[_] = {
    """
      | @param firstData: Takes Data Dataset
      | @param firstInput: Input String for columns that need to removed
      | @return: New Dataset with remaining columns
      |""".stripMargin

    SparkSetup.main(Array.empty[String])
    import SparkSetup.spark.implicits._

    // Split input strings by comma and trim whitespace
    val firstColumns = firstInput.split(",").map(_.trim)

    // Select only the columns you want to keep
    val selectedColumns = firstData.columns.filterNot(firstColumns.contains)
    val transformedData = firstData.select(selectedColumns.head, selectedColumns.tail: _*)

    transformedData
  }

  def stringToSeq(data: Dataset[_], column: String): Dataset[_] = {
    """
      | @param data: Takes Data Dataset
      | @returns: Address column is switched from a String type to ArrayType(StringType)
      |""".stripMargin

    data.withColumn(column, split(col(column), ",").cast(ArrayType(StringType)))
      .withColumn(column, expr("transform("+column+", x -> trim(x))"))
  }

  def removeColumnsAndMergeTwoSetsOfData(firstData: Dataset[_], firstInput: String, secondData: Dataset[_], secondInput: String): Dataset[_] = {
    """
      | @param firstData: Takes Data file as a Dataset
      | @param firstInput: Input String for columns that need to removed
      | @param secondData: Takes Data file as a Dataset
      | @param secondInput: Input String for columns that need to removed
      | @return: Returns new Data Dataset by merging firstData and secondData
      |
      | "Address" is switched from a String type to ArrayType(StringType)
      |""".stripMargin

    val removedColumnsForFirstData = removeColumns(firstData, firstInput)
    val removedColumnsForSecondFile = removeColumns(secondData, secondInput)
    val joinedColumnsThatHaventBeenRemoved = JoinData.joinData(removedColumnsForFirstData, removedColumnsForSecondFile, "customerId")
    val transformAddressToSeq = stringToSeq(joinedColumnsThatHaventBeenRemoved, "address")

    transformAddressToSeq
  }
}
