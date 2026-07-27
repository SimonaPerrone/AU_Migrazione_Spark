package it.eng.au.common

import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities

class testUtilities extends EnvironmentSparkTest{

  def testNumberMonthAgoToDefine() = {

    val window = "5"
    val result = argumentsUtilities.numberMonthAgoToDefine(window)
    println(result)
  }


  def testAnnomeseDefiniton() = {

    val window = "5"
    val timeZone = "UTC"
    val result = argumentsUtilities.annomeseDefiniton(window,timeZone)
    println(result)
  }

  def testGetYearMonthRange(): Unit = {
    val example = 202412
    val result = argumentsUtilities.getYearMonthRange(example)
    println(result)
  }

}
