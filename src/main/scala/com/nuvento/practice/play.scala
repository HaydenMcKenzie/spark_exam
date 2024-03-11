package com.nuvento.practice

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.SetUp.{accountData, addressData, parquetFilePath}
import com.nuvento.sparkexam.comebinedata.Parsing.parse
import com.nuvento.sparkexam.handlefiles.ReadData.readParquetFile
import com.nuvento.sparkexam.handlefiles.Schemas.{accountSchema, addressDataSchema, customerDocument}
import com.nuvento.sparkexam.utils._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{collect_list, struct}


object play extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  //parse(addressData, "address").show()

  val parsedData = parse(addressData, "address")
  parsedData.show(false)

  val addressInfo = readParquetFile(parquetFilePath)
  addressInfo.show()

  """
   def x(Dataset) Dataset =

   parsedData.lift(0)  === customerid
   parsedData.lift(0)  === forename
   parsedData.lift(0)  === surname
   parsedData.lift(0)  === accounts
   addressInfo.lift    === address
   join.as[customerDocument]
  """

}
