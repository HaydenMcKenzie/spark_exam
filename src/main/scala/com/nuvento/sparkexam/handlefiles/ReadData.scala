package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.Schemas._

import scala.reflect.ClassTag

object ReadData extends App {

  import SparkSetup.spark.implicits._

  def readFileData[T: Encoder : ClassTag](filePath: String): Dataset[T] = {
    SparkSetup.spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/" + filePath + ".csv")
      .as[T]
  }

  def readParquetFile(): DataFrame = {
    val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"
    val parquetDF = SparkSetup.spark.read.parquet(parquetFilePath)

    parquetDF
  }

}