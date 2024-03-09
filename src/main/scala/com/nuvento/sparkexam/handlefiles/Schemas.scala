package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{Encoder, Encoders}

object Schemas extends App {
  """
    | List of all schemas that can be used to transform raw data from csv files to build Datasets
    |""".stripMargin

  // customer_data.csv schema
  case class customerSchema(customerId: String, forename: String, surname: String)

  // account_data.csv schema
  case class accountSchema(customerId: String, accountId: String, balance: Double)

  // address_data.csv schema
  case class addressSchema(addressId: String, customerId: String, address: String)

  case class newAccountSchema(
                             customerId: String,
                             forename: String,
                             surname: String,
                             accounts: Array[String],
                             numberAccounts: Integer,
                             totalBalance: Long,
                             averageBalance: Double
                             )

  implicit val customerSchemaEncoder: Encoder[customerSchema] = Encoders.product[customerSchema]
  implicit val accountSchemaEncoder: Encoder[accountSchema] = Encoders.product[accountSchema]
  implicit val addressSchemaEncoder: Encoder[addressSchema] = Encoders.product[addressSchema]
  implicit val newAccountSchemaEncoder: Encoder[newAccountSchema] = Encoders.product[newAccountSchema]
}
