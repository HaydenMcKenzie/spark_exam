package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object FriendsByAgeTest extends App {

  // Schema
  case class Person(id: Int, name: String, age: Int, friends: Int)

  // Main program

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("SparkSQL")
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


  people.groupBy("age").avg("friends").show()

  spark.stop()
}
