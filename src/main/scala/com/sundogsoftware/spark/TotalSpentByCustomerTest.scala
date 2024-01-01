package com.sundogsoftware.spark

import org.apache.spark.sql.types.{FloatType, IntegerType, DoubleType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TotalSpentByCustomerTest extends App {

  case class customerClass(cust_id: Int, other_id: Int, amount_spent: Double)

  //main program
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkSession using every core of the local machine
  val spark = SparkSession
    .builder
    .appName("CustomerSpent")
    .master("local[*]")
    .getOrCreate()

  val customerSchema = new StructType()
    .add("cust_id", IntegerType, nullable = true)
    .add("other_id", IntegerType, nullable = true)
    .add("amount_spent", DoubleType, nullable = true)

  import spark.implicits._
  val ds = spark.read
    .schema(customerSchema)
    .csv("data/customer-orders.csv")
    .as[customerClass]

  val customerSpent = ds.select("cust_id", "amount_spent")

  val avgCustomerSpent = customerSpent
    .groupBy("cust_id")
    .agg(round(sum("amount_spent"), 2)
      .alias("total_spent"))

  val avgCustomerSpentRound = avgCustomerSpent
    .sort("total_spent")

  avgCustomerSpentRound.show(avgCustomerSpentRound.count.toInt)
}
