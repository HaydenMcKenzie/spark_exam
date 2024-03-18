package com.nuvento.sparkexam.handlefiles

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.handlefiles.Schemas._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}
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
    val document = Schemas.CustomerDocument("1", "Alice", "Smith", Seq("ACC001", "ACC002"), Seq("Address1", "Address2"))
    assert(document.customerId == "1")
    assert(document.forename == "Alice")
    assert(document.surname == "Smith")
    assert(document.accounts == Seq("ACC001", "ACC002"))
    assert(document.address == Seq("Address1", "Address2"))
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
    def schemaOf[T](encoder: Encoder[T]): String = {
      encoder.asInstanceOf[ExpressionEncoder[T]].schema.treeString
    }

    val testSchema = schemaOf(customerDocumentEncoder)
    val expectedSchema = schemaOf(Encoders.product[CustomerDocument])
    assert(testSchema == expectedSchema)
  }
}
