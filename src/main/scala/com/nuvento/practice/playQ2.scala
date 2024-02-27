package com.nuvento.practice

import org.apache.spark.sql.SparkSession
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.schemas

object playQ2 extends App {

  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._


  // Define the path to the Parquet file
  val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"

  // Read the Parquet file into a DataFrame
  val parquetDF = SparkSetup.spark.read.parquet(parquetFilePath)

  val addressData = readFileData[schemas.addressSchema]("address_data")

  // Show the schema and some data from the DataFrame
  parquetDF.printSchema()
  parquetDF.show()
  addressData.printSchema()
  addressData.select("address").show()
}
