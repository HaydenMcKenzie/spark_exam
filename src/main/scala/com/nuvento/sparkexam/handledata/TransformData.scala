package com.nuvento.sparkexam.handledata

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handlefiles.Schemas.{CustomerAccountOutput, RawAccountData, RawCustomerData}
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object TransformData extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  """
    | @param customerData: Input Dataset
    | @param accountData: Input Dataset
    | encoder: Encoder[CustomerAccountOutput] from Schemas.scala
    |
    | @return Dataset[CustomerAccountOutput]: Returns new Dataset adding accounts, numberAccounts, totalBalance and averageBalance columns
    |
    | Left join customerData and accountData via customerId
    | Groups by "customerId", "forename", "surname"
    | Collects all account information and puts them into a Seq[RawAccountData]. Renames column to accounts
    | Counts elements in Seq[RawAccountData]. Renames column to numberAccounts
    | Adds all accounts balances to 2 decimal places. Renames column to totalBalance
    | Averages all accounts balances to 2 decimal places. Renames column to averageBalance
    |""".stripMargin

  def aggregatedDataSet(customerData: Dataset[RawCustomerData],accountData: Dataset[RawAccountData])(implicit encoder: Encoder[CustomerAccountOutput]): Dataset[CustomerAccountOutput] = {
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
