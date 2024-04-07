package com.nuvento.sparkexam

import com.nuvento.sparkexam.SetUp.{addressData, parquetFilePath}
import com.nuvento.sparkexam.handledata.Parsing.{createCustomerDocument, parseAddress}
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.handlefiles.Schemas.CustomerDocument
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.Dataset

object QuestionTwo extends App {
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

  try {
    val parquetFile = readParquetFile(parquetFilePath)
    val parsedData = parseAddress(addressData, "address")

    // Show
    val answer: Dataset[CustomerDocument] = questionTwo(parquetFile, parsedData)
    answer.show(1000,false)
    answer.printSchema()

  } catch {
    case e: Exception => println(s"File Does Not Exists.")
  }
}