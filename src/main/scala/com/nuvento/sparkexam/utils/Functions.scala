package com.nuvento.sparkexam.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object Functions {
  def assertDataFrameSchemaEquals(expected: DataFrame, result: DataFrame): Unit = {
    // Testing Schema Matches
    assert(expected.schema == result.schema)
  }

  def assertDataFrameLengthEquals(expected: DataFrame, result: DataFrame): Unit = {
    // Testing Length of Both Rows
    val expectedRows = expected.collect()
    val resultRows = result.collect()

    assert(expectedRows.length == resultRows.length)
  }

  def assertDataFrameSetEquals(expected: DataFrame, result: DataFrame): Unit = {
    // Testing Sets of Both Rows
    val expectedRows = expected.collect()
    val resultRows = result.collect()

    val expectedRowsSet = expectedRows.toSet
    val resultRowsSet = resultRows.toSet

    assert(expectedRowsSet == resultRowsSet)
  }
}
