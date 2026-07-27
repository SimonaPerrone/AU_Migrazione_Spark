package it.eng.au.freezerPreCalcolo.utility

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, YearMonth}

object DateTimeUtility {

  def stringToLocalDate(stringDate: String, formatterPattern: String): LocalDate = {
    val formatter = DateTimeFormatter.ofPattern(formatterPattern)
    LocalDate.parse(stringDate, formatter)
  }

  def stringToLocalDateTime(stringDate: String, formatterPattern: String): LocalDateTime = {
    val formatter = DateTimeFormatter.ofPattern(formatterPattern)
    LocalDateTime.parse(stringDate, formatter)
  }

  def dateToString(localDate: LocalDate, formatterPattern: String): String = {
    val formatter = DateTimeFormatter.ofPattern(formatterPattern)
    localDate.format(formatter)
  }

  def dateToString(localDate: LocalDateTime, formatterPattern: String): String = {
    val formatter = DateTimeFormatter.ofPattern(formatterPattern)
    localDate.format(formatter)
  }

  def isLastDayOfMonth(localDate: LocalDate): Boolean = {
    val month = YearMonth.from(localDate)
    val end = month.atEndOfMonth()
    localDate.equals(end)
  }

  def isFirstDayOfMonth(localDate: LocalDate): Boolean = {
    val firstOfMonth = localDate.withDayOfMonth(1)
    localDate.equals(firstOfMonth)
  }

}
