package com.nuvento.sparkexam.handlefiles

import com.nuvento.sparkexam.handlefiles.Schemas.{accountSchema, customerSchema, addressSchema}
import com.nuvento.sparkexam.utils.SparkSetup
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SchemasTest extends AnyFunSuite with BeforeAndAfter {
  SparkSetup.main(Array.empty[String])

  test("Test customerSchema") {
    // Create a sample instance of customerSchema
    val customer = customerSchema("1", "Alice", "Smith")

    // Verify that the fields are correctly set
    assert(customer.customerId == "1")
    assert(customer.forename == "Alice")
    assert(customer.surname == "Smith")
  }

  test("Test accountSchema") {
    // Create a sample instance of accountSchema
    val account = accountSchema("1", "ACC001", 100.0)

    // Verify that the fields are correctly set
    assert(account.customerId == "1")
    assert(account.accountId == "ACC001")
    assert(account.balance == 100.0)
  }

  test("Test addressSchema") {
    // Create a sample instance of addressSchema
    val address = addressSchema("1", "1", "123 Main St")

    // Verify that the fields are correctly set
    assert(address.addressId == "1")
    assert(address.customerId == "1")
    assert(address.address == "123 Main St")
  }

}
