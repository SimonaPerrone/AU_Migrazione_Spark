package it.au.misure.ingestionMisureGasUnico.utility

import it.au.misure.ingestionMisureGasUnico.utility.Constants.{RCUGAS_PDR_STATO_DATE_TIME_PATTERN, RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS}

import java.time.{LocalDate, LocalDateTime}
import junit.framework.TestCase
import org.junit.Assert

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Locale

class TestDateTimeUtility extends TestCase{

    def testGetDateTimeOr() :Unit = {

    Assert.assertEquals(
       LocalDateTime.MIN
       , DateTimeUtility.getDateTimeOr("aaaaa","yyyy-MM-dd HH:mm:ss.S","min")

    )

    Assert.assertEquals(
      LocalDateTime.MAX
      , DateTimeUtility.getDateTimeOr("aaaaa","yyyy-MM-dd HH:mm:ss.S","max")
    )

    Assert.assertEquals(
      null
      , DateTimeUtility.getDateTimeOr("aaaaa","yyyy-MM-dd HH:mm:ss.S","null")

    )

    Assert.assertEquals(
      LocalDateTime.of(2020,10,10,23,10,1,0)
      , DateTimeUtility.getDateTimeOr("2020-10-10 23:10:01.0","yyyy-MM-dd HH:mm:ss.S","null")
    )

    }

  def testGetDateTimeOrNullOptionalFormat(): Unit = {
    Assert.assertEquals(
      LocalDateTime.of(2022,3,24,11,44,0,0),
      DateTimeUtility.getDateTimeOrNull("2022-03-24 11:44:00.0", RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS)
    )

    Assert.assertEquals(
      LocalDateTime.of(2022,3,24,11,44,0,0),
      DateTimeUtility.getDateTimeOrNull("2022-03-24 11:44:00", RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS)
    )

    Assert.assertEquals(
      null,
      DateTimeUtility.getDateTimeOrNull("2022-03-24 11:44", RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS)
    )

    val dataInizio1 = DateTimeUtility.getDateTimeOr("2022-03-24 11:44:00.0",RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"min")
    val dataFine1 = DateTimeUtility.getDateTimeOr("2050-03-24 11:44:00.0",RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"max")
    val dataInizio2 = DateTimeUtility.getDateTimeOr("2022-03-24 11:44:00",RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"min")
    val dataFine2 = DateTimeUtility.getDateTimeOr("2050-03-24 11:44:00",RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"max")
    val dataInizio3 = DateTimeUtility.getDateTimeOr("2022-03-24 11:44",RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"min")
    val dataFine3 = DateTimeUtility.getDateTimeOr("2022-03-24 11:44",RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"max")

    println(dataInizio1, dataFine1, dataInizio2, dataFine2, dataInizio3, dataFine3)
  }

  def testDateTime(): Unit = {
    val date = "Fri Oct 21 01:27:29 CEST 2022"
    val datetime = LocalDateTime.parse(date,DateTimeFormatter.ofPattern("EE MMM dd HH:mm:ss 'CEST' yyyy", Locale.ENGLISH)).toLocalDate
    println(datetime.toString)
    println(LocalDate.now())
    println(datetime.isEqual(LocalDate.now()))
  }
}
