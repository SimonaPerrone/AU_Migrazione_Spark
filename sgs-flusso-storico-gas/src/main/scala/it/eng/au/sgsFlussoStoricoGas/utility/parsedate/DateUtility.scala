package it.eng.au.sgsFlussoStoricoGas.utility.parsedate

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.joda.time.base.BaseSingleFieldPeriod
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days, Months, Years}

object DateUtility {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def isBetween(date: DateTime, startDate: DateTime, endDate: DateTime): Boolean = {
    (date == startDate || date.isAfter(startDate)) &&
      (date == endDate || date.isBefore(endDate))
  }

  def daysBetween(startDate: DateTime, endDate: DateTime): Int = {
    (startDate, endDate) match {
      case (null, _) => -1
      case (_, null) => -1
      case (s: DateTime, e: DateTime) => Math.abs(Days.daysBetween(e.withTimeAtStartOfDay(), s.withTimeAtStartOfDay()).getDays)
      case (_, _) => -1
    }
  }

  def monthsDifference(startDate: DateTime, endDate: DateTime): Int = {
    (startDate, endDate) match {
      case (null, _) => -1
      case (_, null) => -1
      case (s: DateTime, e: DateTime) =>
        math.max(
          math.abs(Months.monthsBetween(e.withDayOfMonth(1).withTimeAtStartOfDay(), s.withDayOfMonth(1).withTimeAtStartOfDay()).getMonths),
          0
        )
      case (_, _) => -1
    }
  }

  def dateSegmentsIntersects(leftDateSeg1: DateTime, rightDateSeg1: DateTime, leftDateSeg2: DateTime, rightDateSeg2: DateTime): Boolean = {
    //two segment intersects iff they not(do not intersect)
    //two segments do not intersect (in 1D space) iff the former finishes before the latter begins or vice versa
    !(
      (leftDateSeg1.isBefore(leftDateSeg2) && rightDateSeg1.isBefore(leftDateSeg2)) ||
        (leftDateSeg1.isAfter(rightDateSeg2) && rightDateSeg1.isAfter(rightDateSeg2))
      )
  }

  val dateTimeOrdering =
    new Ordering[DateTime] {
      def compare(x: DateTime, y: DateTime): Int = x compareTo y
    }

  def filterDfWithStartEndDate(df: DataFrame, columnStartDateDf: String, columnEndDateDf: String, formatColumnDf: String, startDate: String, endDate: String, formatDate: String): DataFrame = {
    val computationStartDate = lit(DateTimeFormat.forPattern(formatDate)
      .parseDateTime(startDate)
      .dayOfMonth().withMinimumValue().toString("yyyy-MM-dd")
    ).cast(DateType)
    val computationEndDate = lit(DateTimeFormat.forPattern(formatDate)
      .parseDateTime(endDate)
      .dayOfMonth().withMaximumValue().toString("yyyy-MM-dd")
    ).cast(DateType)

    df
      .withColumn(columnStartDateDf, from_unixtime(unix_timestamp(col(columnStartDateDf), formatColumnDf)).cast(DateType))
      .withColumn(columnEndDateDf, from_unixtime(unix_timestamp(col(columnEndDateDf), formatColumnDf)).cast(DateType))
      .withColumn(columnStartDateDf, coalesce(col(columnStartDateDf), from_unixtime(unix_timestamp(lit("1970-01-01 00:00:00.0"), "yyyy-MM-dd HH:mm:ss.S")).cast(DateType)))
      .withColumn(columnEndDateDf, coalesce(col(columnEndDateDf), from_unixtime(unix_timestamp(lit("2999-12-31 00:00:00.0"), "yyyy-MM-dd HH:mm:ss.S")).cast(DateType)))
      .filter(col(columnStartDateDf) <= computationEndDate && col(columnEndDateDf) >= computationStartDate)
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

