package com.nuvento.sparkexam.utils

import org.apache.spark.sql.Dataset

object WriteToFile extends App {

  def writeToFile(aggregated: Dataset[_], outputPath: String): Unit = {
    """
      | @param aggregated: Take Dataset
      | @param outputPath: String for file path
      | @return: Write file or return "File Already Exists"
      |""".stripMargin

    SparkSetup.main(Array.empty[String])
    import SparkSetup.spark.implicits._

    try {
      aggregated.coalesce(1).write.parquet(outputPath)
      println("File Has Been Created.")
    } catch {
      case e: Exception => println(s"File Already Exists")
    }
  }
}
