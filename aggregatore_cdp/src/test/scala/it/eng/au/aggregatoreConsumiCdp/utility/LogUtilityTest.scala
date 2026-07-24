package it.eng.au.aggregatoreConsumiCdp.utility

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest

import java.time.format.DateTimeFormatter

class LogUtilityTest extends EnvironmentSparkTest {
  val applicationName = "Test"
  val logName = "LOG"

  def testPrintInitialLog(): Unit = {
    LogUtility.printInitialLog(applicationName, logName)
  }

  def testPrintFinalLog(): Unit = {
    LogUtility.printFinalLog(applicationName, logName)
  }

  def testGetElapsedTime(): Unit = {
    val startDate = "2022-08-29 10:00:00"
    val endDate = "2022-08-29 19:57:23"
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val (hours, minutes, seconds, endTime) = LogUtility.getElapsedTime(startDate, endDate, dateTimeFormatter)
    println(s"Elapsed time: $hours hours, $minutes minutes and $seconds seconds.")
    println(s"End time: $endTime.")
  }
}
