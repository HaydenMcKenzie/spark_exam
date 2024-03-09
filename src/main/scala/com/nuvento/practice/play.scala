package com.nuvento.practice

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.SetUp.{accountData, customerData}
import org.apache.spark.sql.Dataset
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

}
