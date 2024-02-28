package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{Encoder, Encoders}
import com.nuvento.sparkexam.utils.SparkSetup._
import org.apache.spark.sql.types._

object Schemas extends App {
  // customer_data.csv schema
  case class customerSchemaTestRun(customerId: String, forename: String, surname: String)

  // account_data.csv schema
  case class accountSchema(customerId: String, accountId: String, balance: Double)

  // address_data.csv schema
  case class addressSchema(addressId: String, customerId: String, address: String)



  implicit val customerSchemaEncoder: Encoder[customerSchemaTestRun] = Encoders.product[customerSchemaTestRun]
  implicit val accountSchemaEncoder: Encoder[accountSchema] = Encoders.product[accountSchema]
  implicit val addressSchemaEncoder: Encoder[addressSchema] = Encoders.product[addressSchema]
}
