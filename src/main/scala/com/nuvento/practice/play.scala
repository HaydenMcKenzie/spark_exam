package com.nuvento.practice

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.SetUp.{accountData, addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.JoinData.joinData
import com.nuvento.sparkexam.comebinedata.Parsing.parse
import com.nuvento.sparkexam.comebinedata.TransformData.stringToSeq
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.handlefiles.Schemas.{accountSchema, addressDataSchema, customerSchema}
import com.nuvento.sparkexam.utils._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{array, collect_list, concat_ws, struct, udf}
import com.nuvento.sparkexam.comebinedata.Parsing.customerDocument

object play extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  //parse(addressData, "address").show()

  val parsedData = parse(addressData, "address")
  parsedData.show(false)

  val mergedData = parsedData.withColumn("mergedAddress", array($"addressId", $"customerId",$"address", $"number", $"road", $"city", $"country"))
    .drop("addressID","address", "number", "road", "city", "country")
    .withColumnRenamed("mergedAddress", "address")

  mergedData.show(false)

  val addressInfo = readParquetFile(parquetFilePath)
  addressInfo.show()

  val a = addressInfo.join(parsedData, "customerId")
  a.show(false)
  a.printSchema()

  case class x(customerId: String, forename: String, surname: String, accounts: Seq[String], address: Seq[String])

  def xTest(data1: Dataset[_], mergedData: Dataset[_]): Dataset[_] = {

    val y = data1.join(mergedData, "customerId")

    val r = y.withColumn("mergedAddress", array($"addressId", $"customerId", $"address", $"number", $"road", $"city", $"country"))
      .drop("addressID", "address", "number", "road", "city", "country")
      .withColumnRenamed("mergedAddress", "address")

    r.select(
      $"customerId",
      $"forename",
      $"surname",
      $"accounts",
      $"address"
    )
      .as[x]
  }

  println("xTest")
  xTest(addressInfo, parsedData).show()

  println("customerDocument")
  customerDocument(addressInfo, parsedData).show()
}
