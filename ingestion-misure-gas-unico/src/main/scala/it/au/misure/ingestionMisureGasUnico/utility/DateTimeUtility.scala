package it.au.misure.ingestionMisureGasUnico.utility

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, YearMonth}

import scala.util.{Failure, Success, Try}

object DateTimeUtility {

  def getDateOrNull( date :String, pattern: String ): LocalDate ={

    Try(LocalDate.parse(date,DateTimeFormatter.ofPattern(pattern) )) match{
      case Success(date) =>  date
      case Failure(exception) => null
    }

  }

  def getYearMonthOrNull( yearMonth :String, pattern: String ): YearMonth ={

    Try(YearMonth.parse(yearMonth,DateTimeFormatter.ofPattern(pattern) )) match{
      case Success(ym) =>  ym
      case Failure(exception) => null
    }

  }

  def getDateTimeOrNull( dateTime :String, pattern: String ): LocalDateTime ={

    Try(LocalDateTime.parse(dateTime,DateTimeFormatter.ofPattern(pattern) )) match{
      case Success(dt) =>  dt
      case Failure(exception) => null
    }

  }

  def getDateTimeOr( dateTime :String, pattern: String, or: String ): LocalDateTime ={

    or.toLowerCase() match {
      case "null" => getDateTimeOrNull(dateTime, pattern)
      case "min" => if( getDateTimeOrNull(dateTime, pattern) == null ) LocalDateTime.MIN else getDateTimeOrNull(dateTime, pattern)
      case "max" => if( getDateTimeOrNull(dateTime, pattern) == null ) LocalDateTime.MAX else getDateTimeOrNull(dateTime, pattern)
    }

  }

}
