package com.nuvento.sparkexam

import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.combinedata.JoinData._
import com.nuvento.sparkexam.combinedata.TransformData.{removeColumns, stringToSeq}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

object QuestionTwo extends App {

  SparkSetup.main(Array.empty[String])

  import SparkSetup.spark.implicits._

  val removedColumns = removeColumns(readFileData[Schemas.addressSchema]("address_data"), readParquetFile())

  val transformedData = stringToSeq(removedColumns)

  transformedData.show()
  transformedData.printSchema()
}
