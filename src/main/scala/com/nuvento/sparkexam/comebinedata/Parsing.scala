package com.nuvento.sparkexam.comebinedata

import com.nuvento.practice.play.{AddressData, addressStruct}
import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.comebinedata.TransformData.removeColumns
import com.nuvento.sparkexam.handlefiles.Schemas.AddressSchema
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, collect_list, struct, udf}

object Parsing extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  def parseAddress(data: Dataset[_], addressString: String): Dataset[_] = {
    val extractAddressInfoUDF = udf((address: String) => {
      val parts = address.split(", ")
      val number = parts.headOption.flatMap(part => "\\d+".r.findFirstIn(part).map(_.toInt))
      val road = parts.lift(1)
      val city = parts.lift(2)
      val country = parts.lift(3)
      (number, road, city, country)
    })

    data.withColumn("addressInfo", extractAddressInfoUDF(col(addressString)))
      .select(
        $"addressId",
        $"customerId",
        $"address",
        $"addressInfo._1".alias("number"),
        $"addressInfo._2".alias("road"),
        $"addressInfo._3".alias("city"),
        $"addressInfo._4".alias("country")
      )
      .as[AddressSchema]
  }

  def createCustomerDocument(data: Dataset[_]): Dataset[_] = {
    val addressStruct = struct(
      $"addressId",
      $"customerId",
      $"address",
      $"number",
      $"road",
      $"city",
      $"country"
    )

    data
      .groupBy($"customerId", $"forename", $"surname", $"accounts")
      .agg(collect_list(addressStruct).as("address"))
      .select(
        $"customerId",
        $"forename",
        $"surname",
        $"accounts",
        $"address".as[Seq[AddressData]],
      )
  }
}
