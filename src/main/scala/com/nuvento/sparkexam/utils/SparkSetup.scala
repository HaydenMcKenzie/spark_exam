package com.nuvento.sparkexam.utils

import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object SparkSetup extends App {
  """
    | Logger is set to Level.ERROR to reduce error texts at compilation
    |
    | Starting SparkSession on local device
    |""".stripMargin

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("SparkExam")
    .master("local[*]")
    .getOrCreate()
}
