package com.nuvento.sparkexam.combinedata

import com.nuvento.sparkexam.combinedata.JoinData.joinData
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

object TransformData extends App {
  def aggregatedDataSet(joinedDF: Dataset[_], spark: SparkSession): Dataset[_] = {
    """
      | @param joinedDF: Dataframe from JoinData
      | @param spark: Access SparkSession for certain features needed
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
      countDistinct("accountId").alias("numberAccounts"),
      concat(lit("$"), format_number(sum("balance"), 2)).alias("totalBalance"),
      concat(lit("$"), format_number(round(avg("balance"), 2), 2)).alias("averageBalance")
    )
  }

  def removeColumnsAndMergeIntoOneTable(firstData: Dataset[_], secondData: Dataset[_]): Dataset[_] = {
    val transformData = firstData.drop("addressId")
    val droppedParquet = secondData.drop("numberAccounts", "totalBalance", "averageBalance")

    val joinedData = joinData(droppedParquet,transformData)
    joinedData
  }

  def stringToSeq(data: Dataset[_]): Dataset[_] = {
    data.withColumn("address", split(col("address"), ",").cast(ArrayType(StringType)))
      .withColumn("address", expr("transform(address, x -> trim(x))"))
  }
}
