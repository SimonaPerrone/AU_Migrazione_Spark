package it.eng.au.scambioDatiGasivori.utility

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object LogUtility {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  val applicationName: String = Environment.applicationName
  val logName: String = Environment.logName
  val conf: Config = ConfigFactory.load("version")

  def printVersionInfo(): Unit = {
    log.warn(s"Jar artifactId: ${conf.getString("artifactId")} ")
    log.warn(s"Jar groupId: ${conf.getString("groupId")} ")
    log.warn(s"Jar version: ${conf.getString("version")} ")
    log.warn(s"Last build: ${conf.getString("build.date")} ")
    log.warn(s"Last commit: id -> ${conf.getString("last.commit.id")}, time -> ${conf.getString("last.commit.time")}")
  }

  def printInitialLog(): Unit = {
    log.warn(s"$logName Run $applicationName")
    printVersionInfo()
    log.warn(s"$logName Properties:")
    log.warn(s"$logName ${Environment.printProperties}")
    log.warn(s"$logName Start time: ${Environment.startDateTime.toString}")
    log.warn(s"$logName Execution ID: ${Environment.executionId.toString}")
    log.warn(s"$logName applicationID: ${Environment.sparkContext.applicationId}")
  }

  def printFinalLog(): Unit = {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val startDate = Environment.startDateTime.format(dateFormatter)
    val endDate = LocalDateTime.now().format(dateFormatter)
    val (hours, minutes, seconds, endDateTime) = getElapsedTime(startDate, endDate, dateFormatter)

    log.warn(s"$logName $applicationName job ended.")
    log.warn(s"$logName End time: ${endDateTime.toString}")
    log.warn(s"$logName Time elapsed: $hours hours, $minutes minutes and $seconds seconds.")
  }

  def getElapsedTime(startDate: String, endDate: String, dateFormatter: DateTimeFormatter): (Long, Long, Long, LocalDateTime) = {
    val startDateTime = LocalDateTime.parse(startDate, dateFormatter)
    val endDateTime = LocalDateTime.parse(endDate, dateFormatter)
    val seconds = ChronoUnit.SECONDS.between(startDateTime, endDateTime)
    val hours = seconds / 3600
    val minutes = (seconds % 3600) / 60
    val secs = seconds % 60

    (hours, minutes, secs, endDateTime)
  }
}
