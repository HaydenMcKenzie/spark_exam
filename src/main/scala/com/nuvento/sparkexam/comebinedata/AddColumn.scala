package com.nuvento.sparkexam.comebinedata

import com.nuvento.sparkexam.SetUp.addressData
import com.nuvento.sparkexam.comebinedata.TransformData.stringToSeq
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, concat_ws}

object AddColumn extends App {
  def grabColumn(data: Dataset[_], column: String): Dataset[_] = {
    val testString = stringToSeq(data, column)

    testString.withColumn(column, concat_ws(", ", col("addressId"), col("customerId"), col("address")))
  }
}
