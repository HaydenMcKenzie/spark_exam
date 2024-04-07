package com.nuvento.sparkexam.handlefiles

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handledata.Parsing.parseAddress
import com.nuvento.sparkexam.handlefiles.Schemas._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SchemasTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  Schemas.main(Array.empty[String])

  test("Test RawCustomerSchema") {
    val customer = Schemas.RawCustomerSchema("1", "Alice", "Smith")
    assert(customer.customerId == "1")
    assert(customer.forename == "Alice")
    assert(customer.surname == "Smith")
  }

  test("Test RawAccountSchema") {
    val account = RawAccountSchema("1", "ACC001", 100.0)
    assert(account.customerId == "1")
    assert(account.accountId == "ACC001")
    assert(account.balance == 100.0)
  }

  test("Test RawAddressSchema") {
    val address = RawAddressSchema("1", "1", "123 Main St")
    assert(address.addressId == "1")
    assert(address.customerId == "1")
    assert(address.address == "123 Main St")
  }

  test("Test AddressSchema") {
    val address = AddressSchema("1", "1", "123 Main St", Some(10), Some("Main St"), Some("City"), Some("Country"))
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

    val data1: Dataset[AddressSchema] = Seq(AddressSchema("1", "1", "123 Main St", Some(10), Some("Main St"), Some("City"), Some("Country"))).toDS()
    val addressDataSeq: Dataset[AddressSchema] = parseAddress(data1, "address")

    val document = Schemas.CustomerDocument("1", "Alice", "Smith", Seq("ACC001", "ACC002"), addressDataSeq.collect())
    assert(document.customerId == "1")
    assert(document.forename == "Alice")
    assert(document.surname == "Smith")
    assert(document.accounts == Seq("ACC001", "ACC002"))

    // Extract address strings from AddressSchema objects
    val expectedAddresses = addressDataSeq.collect().map(_.address).toSeq
    assert(document.address.map(_.address) == expectedAddresses) // Compare only the address field
  }

  test("Test CustomerAccountOutput") {
    // Define sample data
    val customerId = "IND0113"
    val forename = "Leonard"
    val surname = "Ball"
    val accounts = Seq(
      RawAccountSchema("IND0113", "ACC0577", 531)
    )
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
    val test = customerSchemaEncoder
    val encoder = Encoders.product[RawCustomerSchema]
    assert(test == encoder)
  }

  test("Test accountSchemaEncoder") {
    val test = accountSchemaEncoder
    val encoder = Encoders.product[RawAccountSchema]
    assert(test == encoder)
  }

  test("Test addressSchemaEncoder") {
    val test = addressSchemaEncoder
    val encoder = Encoders.product[RawAddressSchema]
    assert(test == encoder)
  }

  test("Test addressDataSchemaEncoder") {
    val test = addressDataSchemaEncoder
    val encoder = Encoders.product[AddressSchema]
    assert(test == encoder)
  }

  test("Test customerDocumentEncoder") {
    // Create an instance of the Encoder[CustomerDocument]
    val encoderTest = customerDocumentEncoder
    val encoder = Encoders.product[CustomerDocument]

    // Ensure that the encoder is of the correct type
    assert(encoderTest.toString === encoder.toString)
  }

  test("Test customerAccountOutputEncoder") {
    // Create an instance of the Encoder[CustomerAccountOutput]
    val encoderTest = customerAccountOutputEncoder
    val encoder = Encoders.product[CustomerAccountOutput]

    // Ensure that the encoder is of the correct type
    assert(encoderTest.toString === encoder.toString)
  }
}
