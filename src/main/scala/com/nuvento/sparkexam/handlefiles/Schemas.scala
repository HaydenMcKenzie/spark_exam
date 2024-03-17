package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{Encoder, Encoders}

object Schemas extends App {
  """
    | List of all schemas that can be used to transform raw data from csv files to build Datasets
    |""".stripMargin

  // customer_data.csv schema
  case class RawCustomerSchema(customerId: String, forename: String, surname: String)

  // account_data.csv schema
  case class RawAccountSchema(customerId: String, accountId: String, balance: Double)

  // address_data.csv schema
  case class RawAddressSchema(addressId: String, customerId: String, address: String)

  case class AddressSchema(addressId: String, customerId: String, address: String, number: Option[Int], road: Option[String], city: Option[String], country: Option[String])

  case class CustomerDocument(customerId: String, forename: String, surname: String, accounts: Seq[String], address: Seq[String])


  implicit val customerSchemaEncoder: Encoder[RawCustomerSchema] = Encoders.product[RawCustomerSchema]
  implicit val accountSchemaEncoder: Encoder[RawAccountSchema] = Encoders.product[RawAccountSchema]
  implicit val addressSchemaEncoder: Encoder[RawAddressSchema] = Encoders.product[RawAddressSchema]
  implicit val addressDataSchemaEncoder: Encoder[AddressSchema] = Encoders.product[AddressSchema]
  implicit val customerDocumentEncoder: Encoder[CustomerDocument] = Encoders.product[CustomerDocument]
}
