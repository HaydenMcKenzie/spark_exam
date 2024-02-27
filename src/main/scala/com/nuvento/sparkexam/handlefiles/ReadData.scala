package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{Dataset, SparkSession}
import com.nuvento.sparkexam.utils.SparkSetup._

object ReadData extends App {

  // customer_data.csv schema
  case class customerInfo(customerId: String, forename: String, surname: String)

  // account_data.csv schema
  case class accountInfo(customerId: String, accountId: String, balance: Double)

  import spark.implicits._

  def readCustomerData(): Dataset[customerInfo] = {
    """
      | @returns Dataset of customer_data.csv with customerInfo as a Schema
      |""".stripMargin
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customer_data.csv")
      .as[customerInfo]
  }

  def readAccountData(): Dataset[accountInfo] = {
    """
      | @returns Dataset of account_data.csv with accountInfo as a Schema
      |""".stripMargin
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/account_data.csv")
      .as[accountInfo]
  }

}