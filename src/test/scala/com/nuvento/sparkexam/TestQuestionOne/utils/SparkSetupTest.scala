package com.nuvento.sparkexam.TestQuestionOne.utils

import com.nuvento.sparkexam.utils.SparkSetup
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.{Level, Logger}

class SparkSetupTest extends AnyFunSuite with BeforeAndAfter {

  test("Test SparkSetup") {
    // Set up logger to capture logs
    val logger = Logger.getLogger("org.apache.spark")
    val originalLevel = logger.getLevel

    // Suppress logging to error level
    logger.setLevel(Level.ERROR)

    // Call the SparkSetup object
    SparkSetup.main(Array.empty[String])

    // Check if SparkSession is created
    assert(SparkSetup.spark != null)

    // Check if SparkSession is created with the expected configuration
    if (SparkSetup.spark != null) {
      assert(SparkSetup.spark.sparkContext.master == "local[*]")
    }

    // Restore original logging level
    logger.setLevel(originalLevel)
  }
}
