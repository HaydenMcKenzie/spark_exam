package com.nuvento.sparkexam.TestQuestionOne

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import com.nuvento.practice.playQ1.loadCustomerData



class CountingColumns extends AnyFunSuite with BeforeAndAfterAll  {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("CountingColumnsTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }


  test("Data File Loading") {
    val sampleDF = loadCustomerData(spark, "./data/customer_data.csv")
    val testingCount = sampleDF.count()
    println("testingCount = " + testingCount)
    assert(testingCount == 500, " Record count should be 500")
  }

}
