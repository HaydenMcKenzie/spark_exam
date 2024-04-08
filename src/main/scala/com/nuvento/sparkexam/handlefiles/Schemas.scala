package com.nuvento.sparkexam.handlefiles

import org.apache.spark.sql.{Encoder, Encoders}

object Schemas extends App {
  """
    | A list of Schemas used in throughout the program
    |
    | A list of implicit vals to allow the use throughout the program
    |""".stripMargin

  // Question One File Schemas
  case class RawCustomerData(customerId: String,
                             forename: String,
                             surname: String)

  case class RawAccountData(customerId: String,
                            accountId: String,
                            balance: Double)

  // Question One Final Schema
  case class CustomerAccountOutput(customerId: String,
                                   forename: String,
                                   surname: String,
                                   accounts: Seq[RawAccountData],
                                   numberAccounts: Integer,
                                   totalBalance: Long,
                                   averageBalance: Double)

  // Question One Encoders
  implicit val rawCustomerDataEncoder: Encoder[RawCustomerData] = Encoders.product[RawCustomerData]
  implicit val rawAccountDataEncoder: Encoder[RawAccountData] = Encoders.product[RawAccountData]
  implicit val customerAccountOutputEncoder: Encoder[CustomerAccountOutput] = Encoders.product[CustomerAccountOutput]


  // Question Two File Schemas
  case class RawAddressData(addressId: String,
                            customerId: String,
                            address: String)

  // Extra
  case class AccountData(customerId: String,
                         accountId: String,
                         balance: Double)

  case class AddressData(addressId: String,
                         customerId: String,
                         address: String,
                         number: Option[Int],
                         road: Option[String],
                         city: Option[String],
                         country: Option[String])

  // Question Two Final Schema
  case class CustomerDocument(customerId: String,
                              forename: String,
                              surname: String,
                              accounts: Seq[AccountData],
                              address: Seq[AddressData])

  // Question Two Encoders
  implicit val rawAddressDataEncoder: Encoder[RawAddressData] = Encoders.product[RawAddressData]
  implicit val accountDataEncoder: Encoder[AccountData] = Encoders.product[AccountData]
  implicit val addressDataEncoder: Encoder[AddressData] = Encoders.product[AddressData]
  implicit val customerDocumentEncoder: Encoder[CustomerDocument] = Encoders.product[CustomerDocument]
}
