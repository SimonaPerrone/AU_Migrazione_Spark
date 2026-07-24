package it.eng.au.indennizziMisureGasCommon.utility.date

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.joda.time.base.BaseSingleFieldPeriod
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, Months, Years}

import java.time.YearMonth

object DateUtility {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def getDaysFromMonth(yearMonth: String): String = {
    val year = yearMonth.take(4).toInt
    val month = yearMonth.takeRight(2).toInt
    val yearMonthObject = YearMonth.of(year, month)

    yearMonthObject.lengthOfMonth().toString
  }

  /** When `isInverse` is `false`, select DataFrame records where [`columnStartDateDf`, `columnEndDateDf`] intersects `yearMonth`.
   *
   * When `isInverse` is `true`, select DataFrame records where [ `columnStartDateDf`, `columnEndDateDf`] does not intersect `yearMonth`.
   */
  def intersectDfWithYearMonth(df: DataFrame, columnStartDateDf: String, columnEndDateDf: String, formatColumnDf: String, yearMonth: String, formatDate: String, isInverse: Boolean = false): DataFrame = {
    val computationStartDate = lit(DateTimeFormat.forPattern(formatDate)
      .parseDateTime(yearMonth)
      .dayOfMonth().withMinimumValue().toString("yyyy-MM-dd")
    ).cast(DateType)
    val computationEndDate = lit(DateTimeFormat.forPattern(formatDate)
      .parseDateTime(yearMonth)
      .dayOfMonth().withMaximumValue().toString("yyyy-MM-dd")
    ).cast(DateType)

    val intersectionCondition = if (isInverse) col(columnStartDateDf) > computationEndDate || col(columnEndDateDf) < computationStartDate
    else col(columnStartDateDf) <= computationEndDate && col(columnEndDateDf) >= computationStartDate

    df
      .withColumn(columnStartDateDf, from_unixtime(unix_timestamp(col(columnStartDateDf), formatColumnDf)).cast(DateType))
      .withColumn(columnEndDateDf, from_unixtime(unix_timestamp(col(columnEndDateDf), formatColumnDf)).cast(DateType))
      .withColumn(columnStartDateDf, coalesce(col(columnStartDateDf), from_unixtime(unix_timestamp(lit("1970-01-01 00:00:00.0"), "yyyy-MM-dd HH:mm:ss.S")).cast(DateType)))
      .withColumn(columnEndDateDf, coalesce(col(columnEndDateDf), from_unixtime(unix_timestamp(lit("2999-12-31 00:00:00.0"), "yyyy-MM-dd HH:mm:ss.S")).cast(DateType)))
      .where(intersectionCondition)

      .withColumn(columnStartDateDf, when(col(columnStartDateDf) < computationStartDate, computationStartDate).otherwise(col(columnStartDateDf)))
      .withColumn(columnEndDateDf, when(col(columnEndDateDf) > computationEndDate, computationEndDate).otherwise(col(columnEndDateDf)))
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
