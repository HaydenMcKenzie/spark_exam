package com.nuvento.sparkexam

import com.nuvento.sparkexam.SetUp.addressData
import com.nuvento.sparkexam.handledata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.handlefiles.Schemas.{CustomerAccountOutput, CustomerDocument, RawAddressData}
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.Dataset

object QuestionTwo {
  // Spark Setup
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  def questionTwo(parquetDataInput: Dataset[_], parseAddressDataInput: Dataset[_]): Dataset[CustomerDocument] = {
    """
      | @param parquetDataInput: Data from Question One that has been read into a Dataset from a parquet file
      | @parse parseAddressDataInput: From addressData and the address column
      |
      | Parsed address data is joined into the parquet file data via the customerId column
      | @return Dataset[CustomerDocument]: It is then passed the conjoined data and turns it into Dataset with the schema of CustomerDocument
      |""".stripMargin
    val joinData = parquetDataInput.join(parseAddressDataInput, "customerId")
    val processData = createCustomerDocument(joinData)

    processData
  }

  def answer(parquetFilePath: String, addressData: Dataset[RawAddressData], addressString: String): Dataset[CustomerDocument] = {
    """
      | @param parquetFilePath: Input String for pathway
      | @param addressData: Input Dataset[RawAddressData]
      | @param addressString: Input column for split
      |
      | @try:
      | Create Dataframes via readParquetFile and parseAddress
      | Create answer with Dataset[CustomerDocument]
      | Show answer
      | @catch:
      | Throw error message
      |
      | @return Dataset[CustomerDocument]: a new Dataset with the schema of CustomerDocument
      |""".stripMargin

    try {
      val parquetFile = readParquetFile(parquetFilePath)
      val parsedData = parseAddress(addressData, addressString)

      val answer: Dataset[CustomerDocument] = questionTwo(parquetFile, parsedData)

      answer.show(false)
      answer

    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
        throw e
    }
  }

  def main(args: Array[String]): Unit = {
    // Call App
    answer("output", addressData, "address")
  }
}