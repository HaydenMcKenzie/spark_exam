package com.nuvento.sparkexam.handledata

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handlefiles.Schemas.{AddressData, CustomerDocument}
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{array, col, struct, udf}

object Parsing extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  def parseAddress(data: Dataset[_], addressString: String): Dataset[AddressData] = {
    """
      | @param data: Input for a Dataset
      | @param addressString: Input for the column that needs to be split into multiple columns
      |
      | @return Dataset[AddressSchema]: A new Dataset that splits selected column into separate columns
      |""".stripMargin

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
      .as[AddressData]
  }

  def createCustomerDocument(data: Dataset[_]): Dataset[CustomerDocument] = {
    """
      | @param data: Input Dataset
      |
      | @function aggregated:
      | Select customerId, forename, surname and account
      | Select addressId, customerId, address, number, road, city, country and put it into a array. Alias "address"
      |
      | @return Dataset[CustomerDocument]: A new Dataset that joins the parsed Dataset into a single column and selects certain columns using the AddressSchema Schema
      |""".stripMargin

    val aggregated = data.select(
        $"customerId",
        $"forename",
        $"surname",
        $"accounts",
        array(struct(
          $"addressId",
          $"customerId",
          $"address",
          $"number",
          $"road",
          $"city",
          $"country"
        )).alias("address")
      )

    aggregated.as[CustomerDocument]
  }
}
