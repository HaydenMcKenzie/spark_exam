package com.nuvento.sparkexam

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object play extends App {

  // customer_data.csv
  case class customerInfo(customerId: String, forename: String, surname: String)

  // account_data.csv
  case class accountInfo(customerId: String, accountId: String, balance: Double)

  // joined data
  case class joinedAccount(customerId: String, forename: String, surname: String, accountId: String, balance: Double)


  // Spark Setup
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val cd = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/customer_data.csv")
    .as[customerInfo]

  val ad = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/account_data.csv")
    .as[accountInfo]


  // Code
  val joinedDF = cd.join(ad, "customerId")

  val concatAccountsUDF = udf((accounts: Seq[String]) => accounts.mkString(", "))

  val aggregatedDF = joinedDF.groupBy("customerId", "forename", "surname")
    .agg(
      concatAccountsUDF(collect_list("accountId")).alias("accounts"),
      countDistinct("accountId").alias("numberAccounts"),
      concat(lit("$"), format_number(sum("balance"), 2)).alias("totalBalance"),
      concat(lit("$"), format_number(round(avg("balance"), 2), 2)).alias("averageBalance")
    )


  spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 10)

  aggregatedDF.show()

  aggregatedDF.coalesce(1).write
    .mode("overwrite")
    .csv("src/main/scala/com/nuvento/sparkexam/CustomerAccountOutput")

  spark.stop()

}

