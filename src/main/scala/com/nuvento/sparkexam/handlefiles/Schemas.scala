package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{Encoder, Encoders}

object Schemas extends App {
  """
    | A list of Schemas used in throughout the program
    |
    | A list of implicit vals to allow the use throughout the program
    |""".stripMargin

  case class RawCustomerSchema(customerId: String, forename: String, surname: String)
  case class RawAccountSchema(customerId: String, accountId: String, balance: Double)
  case class RawAddressSchema(addressId: String, customerId: String, address: String)
  case class AddressSchema(addressId: String, customerId: String, address: String, number: Option[Int], road: Option[String], city: Option[String], country: Option[String])
  case class CustomerDocument(customerId: String, forename: String, surname: String, accounts: Seq[RawAccountSchema], address: Seq[AddressSchema])
  case class CustomerAccountOutput(customerId: String, forename: String, surname: String, accounts: Seq[RawAccountSchema], numberAccounts: Integer, totalBalance: Long, averageBalance: Double)


  implicit val customerSchemaEncoder: Encoder[RawCustomerSchema] = Encoders.product[RawCustomerSchema]
  implicit val accountSchemaEncoder: Encoder[RawAccountSchema] = Encoders.product[RawAccountSchema]
  implicit val addressSchemaEncoder: Encoder[RawAddressSchema] = Encoders.product[RawAddressSchema]
  implicit val addressDataSchemaEncoder: Encoder[AddressSchema] = Encoders.product[AddressSchema]
  implicit val customerDocumentEncoder: Encoder[CustomerDocument] = Encoders.product[CustomerDocument]
  implicit val customerAccountOutputEncoder: Encoder[CustomerAccountOutput] = Encoders.product[CustomerAccountOutput]
}
