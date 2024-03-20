package com.nuvento.sparkexam

import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.Dataset

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  def questionTwo(firstDataInput: Dataset[_], secondDataInput: Dataset[_], column: String): Dataset[_] = {
    val joinData = firstDataInput.join(secondDataInput, column)
    val processData = createCustomerDocument(joinData)

    processData
  }

  try {
    val parquetFile = readParquetFile(parquetFilePath)
    val parsedData = parseAddress(addressData, "address")

    // Show
    val answer = questionTwo(parquetFile, parsedData, "customerId")
    answer.show(1000,false)

  } catch {
    case e: Exception => println(s"File Does Not Exists.")
  }
}