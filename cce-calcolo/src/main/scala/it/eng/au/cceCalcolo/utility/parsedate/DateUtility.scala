package it.eng.au.cceCalcolo.utility.parsedate

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, from_unixtime, lit, to_date, unix_timestamp, when}
import org.apache.spark.sql.types.DateType
import org.joda.time.format.DateTimeFormat

object DateUtility {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def fillNullDates(df: DataFrame, StartDate: String, EndDate: String): DataFrame = {
    df
      .withColumn(StartDate, when(col(StartDate).isNull, to_date(lit("1900-01-01"))).otherwise(col(StartDate)))
      .withColumn(EndDate, when(col(EndDate).isNull, to_date(lit("3000-12-31"))).otherwise(col(EndDate)))
  }

}
