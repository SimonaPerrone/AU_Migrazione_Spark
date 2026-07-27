package it.eng.au.queryReport.utility

import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, LogUtility}
import it.eng.au.queryReport.EnvironmentSparkTest
import it.eng.au.queryReport.utility.Constants.TIMESTAMP_FORMAT_WITHOUT_MIL

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class LogUtilityTest extends EnvironmentSparkTest {
  def testPrintInitialLog(): Unit = {
    Environment.setProperty("daterun", new SimpleDateFormat(TIMESTAMP_FORMAT_WITHOUT_MIL).format(Timestamp.valueOf(LocalDateTime.now())))
    LogUtility.printInitialLog("Test", "Test LOG")
  }

  def testPrintFinalLog(): Unit = {
    Environment.setProperty("daterun", new SimpleDateFormat(TIMESTAMP_FORMAT_WITHOUT_MIL).format(Timestamp.valueOf(LocalDateTime.now())))
    LogUtility.printFinalLog("Test", "Test LOG")
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
