package com.nuvento.sparkexam.handlefiles

import com.nuvento.sparkexam.SetUp
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import com.nuvento.sparkexam.utils.SparkSetup

import scala.reflect.ClassTag

object ReadData extends App {
  SetUp.main(Array.empty[String])

  def readFileData[T: Encoder : ClassTag](fileName: String): Dataset[T] = {
    """
      | @param T: Takes a schema
      | @param fileName: Takes a file name
      | @returns: builds a DataSet and implements data from file with the specific schema
      |""".stripMargin

    SparkSetup.spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/" + fileName + ".csv")
      .as[T]
  }

  def readParquetFile(parquetFilePath: String): DataFrame = {
    """
      | @param parquetFilePath: takes file path for parquet folder which is "src/main/scala/com/nuvento/sparkexam/output"
      | @returns: Parquet file that has been read into spark
      |""".stripMargin

    val parquetDF = SparkSetup.spark.read.parquet(parquetFilePath)

    parquetDF
  }

}