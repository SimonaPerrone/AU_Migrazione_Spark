package it.eng.au.aggregatoreConsumiCommon.utility

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class LogUtilityTest extends EnvironmentSparkTest {
  def testPrintInitialLog(): Unit = {
    LogUtility.printInitialLog("Test", "Test LOG")
  }

  def testPrintFinalLog(): Unit = {
    LogUtility.printFinalLog("Test", "Test LOG")
  }

  def testGetElapsedTime(): Unit = {
    val startDate = "2022-08-29 10:00:00.000"
    val endDate = "2022-08-29 19:57:23.223"
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val (hours, minutes, seconds, endTime) = LogUtility.getElapsedTime(startDate, endDate, dateTimeFormatter)
    println(s"Elapsed time: $hours hours, $minutes minutes and $seconds seconds.")
    println(s"End time: $endTime.")
  }
}
