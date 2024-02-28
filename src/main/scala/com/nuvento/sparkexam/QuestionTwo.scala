package com.nuvento.sparkexam

import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.combinedata.TransformData.{removeColumns, stringToSeq}

object QuestionTwo extends App {
  // Spark Setup
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // Transforming the Data
  val removedColumns = removeColumns(readFileData[Schemas.addressSchema]("address_data"), readParquetFile())
  val transformedData = stringToSeq(removedColumns)

  // Printing
  transformedData.show()
  transformedData.printSchema()
}
