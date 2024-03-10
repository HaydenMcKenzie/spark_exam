package com.nuvento.sparkexam.comebinedata

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handlefiles.Schemas.addressDataSchema
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, udf}

object Parsing extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // Apply the UDF to the 'address' column to extract address components
  def processParse(x: Dataset[_], y: String): Dataset[_] = {
    val extractAddressInfoUDF = udf((address: String) => {
      val parts = address.split(", ")
      val number = parts.headOption.flatMap(part => "\\d+".r.findFirstIn(part).map(_.toInt))
      val road = parts.lift(1)
      val city = parts.lift(2)
      val country = parts.lift(3)
      (number, road, city, country)
    })

    x.withColumn("addressInfo", extractAddressInfoUDF(col(y)))
      .select(
        $"addressId",
        $"customerId",
        $"address",
        $"addressInfo._1".alias("number"),
        $"addressInfo._2".alias("road"),
        $"addressInfo._3".alias("city"),
        $"addressInfo._4".alias("country")
      )
      .as[addressDataSchema]
  }
}
