package it.sferanet.au.utilities

import org.apache.log4j.Logger
import org.joda.time.base.BaseSingleFieldPeriod
import org.joda.time.{DateTime, Days, Months, Years}

import java.util.{Calendar, Date}

object DateUtils {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  implicit class DateOps(val date: Date) extends AnyVal {
    def isBetween(start: Date, end: Date): Boolean = {
      (start == date || start.before(date)) && (end == date || end.after(date))
    }
  }

  def getDateWithoutTime(d: Date): Date = {
    val dateNoTime = Calendar.getInstance()
    dateNoTime.setTime(d)
    dateNoTime.set(Calendar.HOUR_OF_DAY, 0)
    dateNoTime.set(Calendar.MINUTE, 0)
    dateNoTime.set(Calendar.SECOND, 0)
    dateNoTime.set(Calendar.MILLISECOND, 0)

    new Date(dateNoTime.getTimeInMillis)
  }

  def convertDateTimeInStringWithFormat(date: DateTime, format: String): String = {
    date.toString(format)
  }

  def convertStringToPeriod(period: String): BaseSingleFieldPeriod = {
    val unit = period.takeRight(1).toLowerCase
    val value = period.dropRight(1).toInt

    unit match {
      case "d" => Days.days(value)
      case "m" => Months.months(value)
      case "y" => Years.years(value)
      case _ => logger.error(s"Error parsing parameter $period. Examples of expected format: 1d, 2m, 5y.")
        throw new Exception(s"Error parsing parameter $period. Examples of expected format: 1d, 2m, 5y.")
    }
  }

}
