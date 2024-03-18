package com.nuvento.sparkexam

import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.utils.SparkSetup

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  try {
    // Raw Data
    val parquetFile = readParquetFile(parquetFilePath)
    val parsedData = parseAddress(addressData, "address")

    // Join Raw Data
    val joinData = parquetFile.join(parsedData, "customerId")

    // Transform
    val processData = createCustomerDocument(joinData)

    // Show
    processData.show(1000,false)

  } catch {
    case e: Exception => println(s"File Does Not Exists.")
  }
}