package it.au.misure.eng.utility

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.util.{Failure, Success, Try}

object DateTimeUtility {

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
