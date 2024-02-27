package com.nuvento.sparkexam.combinedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{collect_list, udf}
import org.apache.spark.sql.functions._

object TransformData extends App {
  def aggregatedDataFrame(joinedDF: DataFrame, spark: SparkSession): DataFrame = {
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

  val concatAccountsUDF = udf((accounts: Seq[String]) => accounts.mkString(", "))

  joinedDF.groupBy("customerId", "forename", "surname")
    .agg(
      concatAccountsUDF(collect_list("accountId")).alias("accounts"),
      countDistinct("accountId").alias("numberAccounts"),
      concat(lit("$"), format_number(sum("balance"), 2)).alias("totalBalance"),
      concat(lit("$"), format_number(round(avg("balance"), 2), 2)).alias("averageBalance")
    )
  }

}
