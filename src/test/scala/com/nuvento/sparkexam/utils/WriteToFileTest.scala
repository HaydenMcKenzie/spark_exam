package com.nuvento.sparkexam.utils

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.utils.WriteToFile.writeToFile
import org.apache.spark.sql.Dataset
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.commons.io.FileUtils

import java.io.File

class WriteToFileTest extends AnyFunSuite with BeforeAndAfter {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._

  private def createTestData(): Dataset[String] = {
    Seq("data1", "data2", "data3").toDS()
  }

  before {
    // Create testoutput file with test data
    val testData = createTestData()
    val outputFilePath = "src/test/scala/com/nuvento/sparkexam/testoutput"
    writeToFile(testData, outputFilePath)
  }

  after {
    // Delete testoutput directory and its contents
    FileUtils.deleteDirectory(new File("src/test/scala/com/nuvento/sparkexam/testoutput"))
  }

  // Test the writeToFile function
  test("Test writeToFile function") {
    // Output path for the test
    val outputFilePath = "src/test/scala/com/nuvento/sparkexam/testoutput"

    // Check if the file has been created
    assert(java.nio.file.Files.exists(java.nio.file.Paths.get(outputFilePath)), "Output file does not exist")
  }
}
