package com.nuvento.sparkexam.handlefiles

object Schemas {

  // Question One File Schemas
  case class RawCustomerData(customerId: String,
                             forename: String,
                             surname: String)

  case class RawAccountData(customerId: String,
                            accountId: String,
                            balance: Integer)

  // Question One Final Schema
  case class CustomerAccountOutput(customerId: String,
                                   forename: String,
                                   surname: String,
                                   accounts: Seq[RawAccountData],
                                   numberAccounts: Integer,
                                   totalBalance: Long,
                                   averageBalance: Double)


  // Question Two File Schemas
  case class RawAddressData(
                             addressId: String,
                            customerId: String,
                            address: String)

  // Extra
  case class AccountData(customerId: String,
                         accountId: String,
                         balance: Integer)

  case class AddressData(addressId: String,
                         customerId: String,
                         address: String,
                         number: Option[Int],
                         road: Option[String],
                         city: Option[String],
                         country: Option[String])

  // Question Two Final Schema
  case class CustomerDocument(
                               customerId: String,
                              forename: String,
                              surname: String,
                              accounts: Seq[AccountData],
                              address: Seq[AddressData])

}