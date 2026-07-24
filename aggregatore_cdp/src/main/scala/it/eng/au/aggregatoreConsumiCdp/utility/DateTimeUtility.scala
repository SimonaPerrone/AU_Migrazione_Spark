package it.eng.au.aggregatoreConsumiCdp.utility

import org.joda.time.format.DateTimeFormat

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DateTimeUtility {
  def getCurrentThermalYear(dateRun: String): String = {
    val dateFormatted = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(dateRun)
    val timestampStartPeriod = dateFormatted.withMonthOfYear(10).withDayOfMonth(1).toDateTime.withTimeAtStartOfDay()

    val startThermalYear =
      if (dateFormatted.isBefore(timestampStartPeriod)) timestampStartPeriod.minusYears(1) else timestampStartPeriod

    startThermalYear.toString("yyyy-MM-dd HH:mm:ss")
  }

  def convertLocalDateTimeToStringWithFormat(localDateTime: LocalDateTime, format: String): String = {
    localDateTime.format(DateTimeFormatter.ofPattern(format))
  }

  def convertStringTimestampToLocalDateTime(localDateTime: String): java.time.LocalDateTime = {
    Timestamp.valueOf(localDateTime).toLocalDateTime
  }
}
