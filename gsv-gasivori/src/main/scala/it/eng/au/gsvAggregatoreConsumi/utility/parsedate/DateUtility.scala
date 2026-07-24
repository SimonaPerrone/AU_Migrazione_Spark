package it.eng.au.gsvAggregatoreConsumi.utility.parsedate

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, from_unixtime, lit, unix_timestamp}
import org.apache.spark.sql.types.DateType
import org.joda.time.format.DateTimeFormat

object DateUtility {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

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

}
