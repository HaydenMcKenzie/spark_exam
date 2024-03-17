package com.nuvento.practice

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.utils._

object play extends App {
  SetUp.main(Array.empty[String])
  import SparkSetup.spark.implicits._
}
