package com.nuvento.sparkexam.utils

// Apache Imports
import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object SparkSetup extends App {
  """
    | Logger is set to Level.ERROR to reduce error texts at compilation
    |
    | Creates SparkSession on local device
    |""".stripMargin

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("SparkExam")
    .config("spark.sql.parquet.compression.codec", "snappy")
    //.config("spark.sql.avro.compression.codec", "gzip")
    .master("local[*]")
    .getOrCreate()
}
