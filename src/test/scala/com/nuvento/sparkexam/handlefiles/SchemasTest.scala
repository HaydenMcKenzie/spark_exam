package com.nuvento.sparkexam.handlefiles

// Nuvento Imports
import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handledata.Parsing.parseAddress
import com.nuvento.sparkexam.handlefiles.Schemas._

// Apache Imports
import org.apache.spark.sql.Dataset

// ScalaTest Imports
import org.scalatest.funsuite.AnyFunSuite


class SchemasTest extends AnyFunSuite {
  SetUp.main(Array.empty[String])

  test("Test RawCustomerData") {
    val customer = RawCustomerData("1", "Alice", "Smith")
    assert(customer.customerId == "1")
    assert(customer.forename == "Alice")
    assert(customer.surname == "Smith")
  }

  test("Test RawAccountData") {
    val account = RawAccountData("1", "ACC001", 100)
    assert(account.customerId == "1")
    assert(account.accountId == "ACC001")
    assert(account.balance == 100.0)
  }

  test("Test CustomerAccountOutput") {
    // Define sample data
    val customerId = "IND0113"
    val forename = "Leonard"
    val surname = "Ball"
    val accounts = Seq(AccountData("IND0113", "ACC0577", 531))
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

  test("Test RawAddressData") {
    val address = RawAddressData("1", "1", "123 Main St")
    assert(address.addressId == "1")
    assert(address.customerId == "1")
    assert(address.address == "123 Main St")
  }

  test("Test AccountData") {
    val account = AccountData("1", "ACC001", 100)
    assert(account.customerId == "1")
    assert(account.accountId == "ACC001")
    assert(account.balance == 100.0)
  }


  test("Test AddressData") {
    val address = AddressData("1", "1", "123 Main St", Some(10), Some("Main St"), Some("City"), Some("Country"))
    assert(address.addressId == "1")
    assert(address.customerId == "1")
    assert(address.address == "123 Main St")
    assert(address.number.contains(10))
    assert(address.road.contains("Main St"))
    assert(address.city.contains("City"))
    assert(address.country.contains("Country"))
  }

  test("Test Adddress") {
    val address = Adddress("1", "1", "123 Main St", Some(10), Some("Main St"), Some("City"), Some("Country"))
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

    val data1: Dataset[Adddress] = Seq(Adddress("1", "1", "123 Main St", Some(10), Some("Main St"), Some("City"), Some("Country"))).toDS()
    val addressDataSeq: Dataset[Adddress] = parseAddress(data1, "address").as[Adddress]

    val accountIds: Seq[AccountData] = Seq(
      AccountData("1", "ACC001", 0),
      AccountData("1", "ACC002", 0)
    )

    val document = Schemas.CustomerDocument("1", "Alice", "Smith", accountIds, addressDataSeq.collect())

    // Extract address strings from AddressSchema objects
    val expectedAddresses = addressDataSeq.collect().map(_.address).toSeq
    assert(document.address.map(_.address) == expectedAddresses) // Compare only the address field
  }

  // Other Tests
  test("Test Schemas Object") {
    assert(Schemas.isInstanceOf[Schemas.type])
  }
}