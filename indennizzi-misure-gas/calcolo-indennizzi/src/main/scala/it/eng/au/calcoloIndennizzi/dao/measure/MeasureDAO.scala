package it.eng.au.calcoloIndennizzi.dao.measure

import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParser}

import scala.util.Try

trait MeasureDAO extends DAO {
  val tableName: String

  def readTable: DataFrame = {
    Environment.spark.read.table(tableName)
  }
}

object MeasureDAO {
  val genericDateTimeParsers: Array[DateTimeParser] = Array(
    DateTimeFormat.forPattern("yyyy-MM-dd").getParser,
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").getParser
  )
  val genericDateTimeFormatter: DateTimeFormatter = new DateTimeFormatterBuilder().append(null, genericDateTimeParsers).toFormatter

  def parseDateToOption(value: String, formatter: DateTimeFormatter = genericDateTimeFormatter): Option[DateTime] = Try(formatter.parseDateTime(value)).toOption
}
