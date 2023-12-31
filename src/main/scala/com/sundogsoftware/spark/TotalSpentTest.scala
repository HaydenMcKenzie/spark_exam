package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object TotalSpentTest extends App {

  def parsedLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "TotalSpentTest")

  val lines = sc.textFile("data/customer-orders.csv")

  val mappedInput = lines.map(parsedLine)

  val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )

  val totalByCustomerFlipped = totalByCustomer.map( x => (x._2, x._1) )

  val totalByCustomerSorted = totalByCustomerFlipped.sortByKey()

  val results = totalByCustomerSorted.collect()

  for (result <- results) {
    val spent = result._1
    val id = result._2
    val dollar = "$"
    val spentNum = f"$dollar$spent%.2f"
    println(s"$id spent that most at: $spentNum")
  }

}
