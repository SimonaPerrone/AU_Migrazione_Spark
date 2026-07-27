package it.au.misure.ee_switching.model.schema.xml

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.au.misure.ee_switching.model.schema.hive.PodMetadata
import it.au.misure.ee_switching.utility.Constants

case class TagXml(
                 tagName: String = null,
                 onlyOpening: Boolean = false,
                 onlyClosure: Boolean = false,
                 infoFrom: String = null,
                 nvlDefaultValue: String = null,
                 readyTag: Boolean = false,
                 stringTransformation: (String) => (String) = null,
                 toItalianDate: Boolean = false,
                 doubleToInt: Boolean = false,
                 formatDouble: Boolean = false,
                 onlyPositiveValue: Boolean = false,
                 emptyTagIfNegativeValue: Boolean = false,
                 presenceCondition: (PodMetadata) => Boolean = (meta) => true
) {

  def getItalianDateFormat(date: String): String = try { LocalDate.parse(date, DateTimeFormatter.ISO_LOCAL_DATE).format(DateTimeFormatter.ofPattern(Constants.ITALIAN_DATE_PATTERN)) } catch { case _: Exception => "" }
  def getIntFromDouble(s: String): String = try { s.toDouble.toInt.toString } catch { case _: Exception => "" }
  def getFormattedDouble(s: String): String =
    try {
      val number: String = "%.3f".format(s.toDouble).replace(".",",")
      if (onlyPositiveValue) {
        if (s.toDouble >= 0) number else ""
      } else
        number
    } catch { case _: Exception => "" }
}