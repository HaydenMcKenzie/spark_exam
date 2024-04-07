package com.nuvento.sparkexam.handledata

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handlefiles.Schemas.CustomerAccountOutput
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, IntegerType, LongType, StringType}

object TransformData extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // Question 1 Functions
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

  def aggregatedDataSet(customerData: Dataset[_],accountData: Dataset[_])(implicit encoder: Encoder[CustomerAccountOutput]): Dataset[CustomerAccountOutput] = {
    val joinDataByCustomerId = customerData.join(accountData, Seq("customerId"), "left")

    joinDataByCustomerId.groupBy("customerId", "forename", "surname")
      .agg(
        // Conditionally collect list based on whether there are accounts with accountId and balance
        collect_list(
          when(col("accountId").isNull && col("balance").isNull, lit(null))
            .otherwise(struct("customerId", "accountId", "balance"))
        ).alias("accounts"),
        countDistinct("accountId").cast(IntegerType).alias("numberAccounts"),
        sum("balance").cast(LongType).alias("totalBalance"),
        round(avg("balance"), 2).alias("averageBalance")
      )
      .as[CustomerAccountOutput]
  }
}
