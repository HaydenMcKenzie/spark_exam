package com.nuvento.sparkexam.utils

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile
import org.apache.spark.sql.Dataset
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class WriteToFileTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  // Function to create test data
  private def createTestData(): Dataset[String] = {
    Seq("data1", "data2", "data3").toDS()
  }

  // Test the writeToFile function
  test("Test writeToFile function") {
    // Input test data
    val testData = createTestData()

    // Output path for the test
    val outputFilePath = "src/test/scala/com/nuvento/sparkexam/testoutput"

    // Call the function
    writeToFile(testData, outputFilePath)

    // Check if the file has been created
    assert(java.nio.file.Files.exists(java.nio.file.Paths.get(outputFilePath)), "Output file does not exist")
  }
}
