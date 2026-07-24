package it.eng.au.pubblicazionePcg.utility

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DateTimeUtility {

  def convertLocalDateTimeToStringWithFormat(localDateTime: LocalDateTime, format: String): String = {
    localDateTime.format(DateTimeFormatter.ofPattern(format))
  }

  def convertStringTimestampToLocalDateTime(localDateTime: String): LocalDateTime = {
    Timestamp.valueOf(localDateTime).toLocalDateTime
  }

}
