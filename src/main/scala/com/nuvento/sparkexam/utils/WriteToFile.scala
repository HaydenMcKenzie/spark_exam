package com.nuvento.sparkexam.utils

import com.nuvento.sparkexam.SetUp
import org.apache.spark.sql.Dataset

object WriteToFile extends App {
  SetUp.main(Array.empty[String])

  def writeToFile(aggregated: Dataset[_], outputPath: String): Unit = {
    """
      | @param aggregated: Take input Dataset that will saved to parquet file
      | @param outputPath: String for file path
      |
      | @return: New file is created with "File has been created" or returns "File Already Exists" if file is already created
      |""".stripMargin

    try {
      aggregated.coalesce(1).write.parquet(outputPath)
      println("File Has Been Created.")
    } catch {
      case e: Exception => println(s"File Already Exists.")
    }
  }
}
