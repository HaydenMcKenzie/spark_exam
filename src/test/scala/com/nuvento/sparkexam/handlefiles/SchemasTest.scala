package com.nuvento.sparkexam.handlefiles

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handledata.Parsing.parseAddress
import com.nuvento.sparkexam.handlefiles.Schemas._
import org.apache.spark.sql.{Dataset, Encoders}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SchemasTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  Schemas.main(Array.empty[String])

  test("Test RawCustomerSchema") {
    val customer = Schemas.RawCustomerData("1", "Alice", "Smith")
    assert(customer.customerId == "1")
    assert(customer.forename == "Alice")
    assert(customer.surname == "Smith")
  }

  test("Test RawAccountSchema") {
    val account = RawAccountData("1", "ACC001", 100.0)
    assert(account.customerId == "1")
    assert(account.accountId == "ACC001")
    assert(account.balance == 100.0)
  }

  test("Test RawAddressSchema") {
    val address = RawAddressData("1", "1", "123 Main St")
    assert(address.addressId == "1")
    assert(address.customerId == "1")
    assert(address.address == "123 Main St")
  }

  test("Test AddressSchema") {
    val address = AddressData("1", "1", "123 Main St", Some(10), Some("Main St"), Some("City"), Some("Country"))
    assert(address.addressId == "1")
    assert(address.customerId == "1")
    assert(address.address == "123 Main St")
    assert(address.number.contains(10))
    assert(address.road.contains("Main St"))
    assert(address.city.contains("City"))
    assert(address.country.contains("Country"))
  }

  test("Test CustomerDocument") {
    import com.nuvento.sparkexam.utils.SparkSetup.spark.implicits._

    val data1: Dataset[AddressData] = Seq(AddressData("1", "1", "123 Main St", Some(10), Some("Main St"), Some("City"), Some("Country"))).toDS()
    val addressDataSeq: Dataset[AddressData] = parseAddress(data1, "address")

    val accountIds: Seq[AccountData] = Seq(
      AccountData("1", "ACC001", 0.0),
      AccountData("1", "ACC002", 0.0)
    )

    val document = Schemas.CustomerDocument("1", "Alice", "Smith", accountIds, addressDataSeq.collect())

    // Extract address strings from AddressSchema objects
    val expectedAddresses = addressDataSeq.collect().map(_.address).toSeq
    assert(document.address.map(_.address) == expectedAddresses) // Compare only the address field
  }

  test("Test CustomerAccountOutput") {
    // Define sample data
    val customerId = "IND0113"
    val forename = "Leonard"
    val surname = "Ball"
    val accounts = Seq(RawAccountData("IND0113", "ACC0577", 531))
    val numberAccounts = 1
    val totalBalance = 531L
    val averageBalance = 531.0

    val customerAccountOutput = CustomerAccountOutput(customerId, forename, surname, accounts, numberAccounts, totalBalance, averageBalance)

    // Assert fields of CustomerAccountOutput
    assert(customerAccountOutput.customerId === customerId)
    assert(customerAccountOutput.forename === forename)
    assert(customerAccountOutput.surname === surname)
    assert(customerAccountOutput.accounts === accounts)
    assert(customerAccountOutput.numberAccounts === numberAccounts)
    assert(customerAccountOutput.totalBalance === totalBalance)
    assert(customerAccountOutput.averageBalance === averageBalance)
  }

  test("Test customerSchemaEncoder") {
    val test = Encoders.product[RawCustomerData]
    assert(rawCustomerDataEncoder.equals(test))
  }

  test("Test rawAccountDataEncoder") {
    val test = Encoders.product[RawAccountData]
    assert(rawAccountDataEncoder.equals(test))
  }

  test("Test accountDataEncoder") {
    val test = Encoders.product[AccountData]
    assert(accountDataEncoder.equals(test))
  }

  test("Test addressDataEncoder") {
    val test = Encoders.product[AddressData]
    assert(addressDataEncoder.equals(test))
  }

  test("Test customerDocumentEncoder") {
    val test = Encoders.product[CustomerDocument]
    assert(customerDocumentEncoder.schema == test.schema)
  }

  test("Test customerAccountOutputEncoder") {
    val test = Encoders.product[CustomerAccountOutput]
    assert(customerAccountOutputEncoder.schema == test.schema)
  }
}