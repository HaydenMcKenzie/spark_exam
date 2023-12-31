package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsByAgeTest extends App {

  // Schema
  case class Person(id: Int, name: String, age: Int, friends: Long)

  // Main program

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("FriendsByAge")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val people = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/fakefriends.csv")
    .as[Person]

  println("Here is our inferred schema:")
  people.printSchema()

  println("My Answer")
  people.groupBy("age").avg("friends").show()

  // Need to add import functions and sparksession
  val friendsByAge = people.select("age", "friends")

  println("Sort By Age")
  friendsByAge.groupBy("age").avg("friends").sort("age").show()

  friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
    .sort("age").show()

  friendsByAge.groupBy("age").agg(round(avg("friends"), 2)
    .alias("friends_avg")).sort("age").show()

  spark.stop()
}
