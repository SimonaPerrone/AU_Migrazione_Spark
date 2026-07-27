package it.eng.au.ERP.utility.functions

import org.apache.spark.sql.SparkSession

import java.time.YearMonth
import java.time.format.DateTimeFormatter

object argumentsUtilities {

  private val ymFormatter = DateTimeFormatter.ofPattern("yyyyMM")
  private val ymdFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def readPodExcluded(hdfsPath: String)(implicit spark:SparkSession): List[String] = {
    // Get the SparkContext from SparkSession
    val sc = spark.sparkContext
    // Read file
    val lines = sc.textFile(hdfsPath)
    // Split by comma and flatten
    val strings = lines.flatMap(_.split(",")).map(_.trim)
    // Collect to driver
    strings.collect().toList
  }

  def yearMonth(annomese: Option[String]): Option[(Int, Int)] = {
    annomese
      .filter(_.matches("""\d{6}""")) // must be exactly 6 digits
      .flatMap { value =>
        val intValue = value.toInt
        val year = intValue / 100
        val month = intValue % 100

        if (month >= 1 && month <= 12)
          Some((year, month))
        else
          None
      }
  }

  def yearMonthMinusOneMonth(year: Int, month:Int): (Int, Int) = {
            if (month == 12) {
              return (year-1,1)

            }
            else
              return (year,month-1)
      }

  def startInFunction(annomese: Option[String]):String = {

    val ym = YearMonth.parse(annomese.get, ymFormatter)

    val prevMonth = ym.minusMonths(1)

    prevMonth.atEndOfMonth().format(ymdFormatter)
  }

  def stopInFunction(annomese: Option[String]):String = {

    val ym = YearMonth.parse(annomese.get, ymFormatter)

    ym.atEndOfMonth().format(ymdFormatter)
  }

  def lstDayPreviousDay(annomese: Option[String]):Int = {

    val ym = YearMonth.parse(annomese.get, ymFormatter)

    val prevMonth = ym.minusMonths(1)

    val prevDayAndMonth = prevMonth.atEndOfMonth()

    prevDayAndMonth.getDayOfMonth
  }

}
