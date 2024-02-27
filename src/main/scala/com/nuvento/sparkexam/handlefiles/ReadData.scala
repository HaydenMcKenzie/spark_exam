package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import com.nuvento.sparkexam.utils.SparkSetup._
import com.nuvento.sparkexam.handlefiles.schemas._
import scala.reflect.ClassTag

object ReadData extends App {

  import spark.implicits._

  def readFileData[T: Encoder : ClassTag](filePath: String): Dataset[T] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/" + filePath + ".csv")
      .as[T]
  }
}