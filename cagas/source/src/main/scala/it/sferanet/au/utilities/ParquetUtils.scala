package it.sferanet.au.utilities

import it.sferanet.au.model.FlowFields
import org.apache.spark.sql.Row

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Try

object ParquetUtils {

  def getString(fieldName: String, row: Row): String = {
    row.getAs[String](fieldName)
  }

  def getChar(fieldName: String, row: Row): Char = {
    row.getAs[String](fieldName).head
  }

  def getInt(fieldName: String, row: Row): Int = {
    row.getAs[String](fieldName).toInt
  }

  def getOptionString(fieldName: String, row: Row): Option[String] = {
    Option(row.getAs[String](fieldName))
  }

  def getOptionInt(fieldName: String, row: Row): Option[Int] = {
    if (row.getAs[String](fieldName) == null) None else Some(row.getAs[String](fieldName) toInt)
  }

  def getOptionDouble(fieldName: String, row: Row): Option[Double] = {
    if (row.getAs[String](fieldName) == null || row.getAs[String](fieldName) == "") None else Some(row.getAs[String](fieldName) toDouble)
  }

  def getEsito(fieldName: String, row: Row): Option[Char] = {
    val esito = if (Try(row.getAs[String](fieldName).trim.equalsIgnoreCase("null")).getOrElse(true)) "" else row.getAs[String](fieldName)

    Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
  }

  def getOptionChar(fieldName: String, row: Row): Option[Char] = {
    if (row.getAs[String](fieldName) == null || row.getAs[String](fieldName).isEmpty) None else Some(row.getAs[String](fieldName).head)
  }

  def getOptionDate(fieldName: String, format: SimpleDateFormat, row: Row): Option[Date] = {
    Constants.getDate(format, row.getAs(fieldName))
  }

  /**
   *
   * @param fieldCheck campo da controllare
   * @param rowMeasure oggetto di tipo[[Row]]
   * @return [[Boolean]] true= indica la presenza o se non è nullo  [[fieldCheck]]
   *         false= indica la non presenza o se è nullo
   */
  def fieldIsNotNull(fieldCheck: String, rowMeasure: Row): Boolean = {
    rowMeasure.getAs(fieldCheck) != null
  }

  def getSerialNumberMisMisureGiornaliere(row: Row, flowFieldsNewRoute: FlowFields, flowFieldsOldRoute: FlowFields): Option[String] = {
    if (getOptionString(flowFieldsNewRoute.serialNumberMis, row).isDefined)
      getOptionString(flowFieldsNewRoute.serialNumberMis, row)
    else
      getOptionString(flowFieldsOldRoute.serialNumberMis, row)
  }

  def getSerialNumberConvMisureGiornaliere(row: Row, flowFieldsNewRoute: FlowFields, flowFieldsOldRoute: FlowFields): Option[String] = {
    if (getOptionString(flowFieldsNewRoute.serialNumberConv, row).isDefined)
      getOptionString(flowFieldsNewRoute.serialNumberConv, row)
    else
      getOptionString(flowFieldsOldRoute.serialNumberConv, row)
  }

}
