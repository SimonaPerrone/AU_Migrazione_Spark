package it.eng.au.cceCalcolo.component.utility

import it.eng.au.cceCalcolo.args.{ArgsFactory, CalcoloArgs}
import it.eng.au.cceCalcolo.utility.EnvironmentSparkTest
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.log.LogUtility

import java.time.format.DateTimeFormatter

class LogUtilityTest extends EnvironmentSparkTest {
  def testPrintInitialLog(): Unit = {
    val args = Array("-a", "2024", "-m", "1", "-t", "PR", "-f", "true")
    implicit val parsedArgs: CalcoloArgs = ArgsFactory.parse(args)
    implicit val executionId: Long = Environment.startDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toLong

    LogUtility.printInitialLog()
  }

  def testPrintFinalLog(): Unit = {
    LogUtility.printFinalLog()
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
