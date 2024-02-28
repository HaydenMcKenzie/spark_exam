package com.nuvento.practice

import org.apache.spark.sql.{Dataset, SparkSession}
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.handlefiles.Schemas._
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object playQ2 extends App {

  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._


  // Define the path to the Parquet file
  val parquetFilePath = "src/main/scala/com/nuvento/sparkexam/output"

  // Read the Parquet file into a DataFrame
  val parquetDF = SparkSetup.spark.read.parquet(parquetFilePath)



  val addressSchema = StructType(Seq(
    StructField("addressId", StringType, nullable = false),
    StructField("customerId", StringType, nullable = false),
    StructField("address", StringType, nullable = false)
  ))

  // Read the data as a DataFrame
  val rawData = SparkSetup.spark.read.schema(addressSchema).csv("data/address_data.csv")

  // Transform the array field into a delimited string
  val processedData = rawData.withColumn("address", concat_ws(",", col("address")))

  """
    | I need to use this processedData and change the old to the new
    | This is because it allows for arrays to be convert from a CSV file.
    |""".stripMargin

  val addressData = readFileData[Schemas.addressSchema]("address_data")

  //val addressData = readFileData2[Schemas.addressSchema]("address_data", addressSchema)
    //readFileData[Schemas.addressSchema]("address_data")
  //val joinedData: Dataset[T] = joinedData(parquetDF, addressData)

  // Show the schema and some data from the DataFrame
  parquetDF.printSchema()
  parquetDF.show()
  addressData.show()
  addressData.printSchema()
  //processedData.select("address").show()
}
