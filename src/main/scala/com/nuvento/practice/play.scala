package com.nuvento.practice

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.SetUp.{accountData, addressData, customerData}
import com.nuvento.sparkexam.comebinedata.TransformData.stringToSeq
import com.nuvento.sparkexam.utils.SparkSetup
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object play extends App {
  SetUp.main(Array.empty[String])

  def aggregatedDataSet(data: Dataset[_]): Dataset[_] = {
    data.groupBy("customerId", "forename", "surname")
      .agg(
        collect_list("accountId").alias("accounts"),
        countDistinct("accountId").cast(IntegerType).alias("numberAccounts"),
        sum("balance").cast(LongType).alias("totalBalance"),
        round(avg("balance"), 2).alias("averageBalance")
      )
  }

  // Left Join it?
  val joinedData = customerData.join(accountData, Seq("customerId"), "left")
  val test  = aggregatedDataSet(joinedData)
  val filteredData = test.filter(col("customerId") === "IND0001")


  test.show()
  println(test.count())
  filteredData.show()

  val testString = stringToSeq(addressData, "address")
  val grabColumn = testString.select("address").toDF()
  grabColumn.collect()

  val grabColumnTest = testString.withColumn("address", concat_ws(", ", col("addressId"), col("customerId"), col("address")))
  println("AddressData")
  val x = grabColumnTest.select("address").collect()
  x.take(2).foreach(println)

  """
  import org.apache.spark.sql.functions.{col, lit}


  val addressDF = grabColumn
    .select(
      col("address").getItem(0).cast("int").alias("number"),
      col("address").getItem(1).alias("road"),
      col("address").getItem(2).alias("city"),
      col("address").getItem(3).alias("country")
    )
    .withColumn("number", when(col("number").isNull, lit(null)).otherwise(col("number")))
    .withColumn("road", when(col("road").isNull, lit(null)).otherwise(col("road")))
    .withColumn("city", when(col("city").isNull, lit(null)).otherwise(col("city")))
    .withColumn("country", when(col("country").isNull, lit(null)).otherwise(col("country")))

  println("New")
  addressDF.show()
  """
}
