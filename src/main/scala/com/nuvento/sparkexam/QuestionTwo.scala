package com.nuvento.sparkexam

import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.handledata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.Dataset

object QuestionTwo extends App {
  // Spark Setup
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  def questionTwo(parguetDataInput: Dataset[_], parseAddressDataInput: Dataset[_]): Dataset[_] = {
    """
      | @param parseAddressDataInput: Data from Question One that has been read into a Dataset from a parguet file
      | @parse AddressDataInput: Address data from address_data.csv
      |
      | Parsed address data is joined into the parguet file data via the customerId column
      | It is then passed the conjoined data and turns it into Dataset with the schema of CustomerDocument
      |""".stripMargin
    val joinData = parguetDataInput.join(parseAddressDataInput, "customerId")
    val processData = createCustomerDocument(joinData)

    processData
  }

  try {
    val parquetFile = readParquetFile(parquetFilePath)
    val parsedData = parseAddress(addressData, "address")

    // Show
    val answer = questionTwo(parquetFile, parsedData)
    answer.show(1000,false)

  } catch {
    case e: Exception => println(s"File Does Not Exists.")
  }
}