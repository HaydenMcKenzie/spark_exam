package com.nuvento.practice

import com.nuvento.sparkexam.SetUp
import com.nuvento.sparkexam.SetUp.addressData
import com.nuvento.sparkexam.comebinedata.Parsing.processParse


object play extends App {
  SetUp.main(Array.empty[String])

  processParse(addressData, "address").show()
}
