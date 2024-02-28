package com.nuvento.sparkexam

import com.nuvento.sparkexam.utils.SparkSetup
import com.nuvento.sparkexam.handlefiles.ReadData._
import com.nuvento.sparkexam.handlefiles.Schemas
import com.nuvento.sparkexam.combinedata.JoinData._
import com.nuvento.sparkexam.combinedata.TransformData.removeColumns

object QuestionTwo extends App {

  SparkSetup.main(Array.empty[String])

  import SparkSetup.spark.implicits._

  val removedColumns = removeColumns(readFileData[Schemas.addressSchema]("address_data"), readParquetFile())

  removedColumns.show()
  removedColumns.printSchema()
}
