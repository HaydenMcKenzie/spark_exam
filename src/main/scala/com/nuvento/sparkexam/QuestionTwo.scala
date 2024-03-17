package com.nuvento.sparkexam

import com.nuvento.sparkexam.comebinedata.TransformData.removeColumns
import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.Parsing.parseAddress
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.handlefiles.Schemas.CustomerDocument
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.functions.array

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  try {
    // Data
    val parquetFile = readParquetFile(parquetFilePath)
    val parsedData = parseAddress(addressData, "address")

    // Transforming the Data
    val joinData = parquetFile.join(parsedData, "customerId")
    val transformData = joinData.withColumn("mergedAddress", array($"addressId", $"customerId", $"address", $"number", $"road", $"city", $"country"))

    val processData = removeColumns(transformData, "addressID, address, number, road, city, country, numberAccounts, totalBalance, averageBalance")
      .drop("addressID")
      .withColumnRenamed("mergedAddress", "address")

    val createCustomerDocument = processData.select($"customerId", $"forename", $"surname", $"accounts", $"address").as[CustomerDocument]

    // Show
    createCustomerDocument.show(false)

  } catch {
    case e: Exception => println(s"File Does Not Exists.")
  }
}