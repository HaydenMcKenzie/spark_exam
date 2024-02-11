package com.nuvento.sparkexam

import org.apache.spark.sql._
import org.apache.log4j._

object QuestionOne extends App {

  // customer_data.csv
  case class customerInfo(customerId: String, forename: String, surname: String)

  // account_data.csv
  case class accountInfo(customerId: String, accountId: String, balance: Double)

  case class joinedAccount(customerId: String, forename: String, surname: String, accountId: String, balance: Double)

  // final
  case class CustomerAccountOutput(customerId: String, forename: String, surname: String,
                                   accounts: Seq[accountInfo], numberAccounts: BigInt, totalBalance: Long,
                                   averageBalance: Double)




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

    /**
    val x = ad.groupBy("customerId").count()

    val connect = cd
      .join(ad, usingColumn = "customerId")
      .join(x, usingColumn = "customerId").where(cd("customerId") === x("customerId"))
    **/


   def jointwoDS (customerInfoDS: Dataset[customerInfo], accountInfoDS: Dataset[accountInfo]): Dataset[joinedAccount] = {
     customerInfoDS.join(accountInfoDS, "customerId").as[joinedAccount]
   }


    val resultDS = jointwoDS(cd, ad)
    resultDS.show(500)


    spark.stop()
}