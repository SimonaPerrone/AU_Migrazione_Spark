package it.eng.au.portale_consumi_ee.utility.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import java.time.YearMonth
import java.util.{Calendar, TimeZone}

object argumentsUtilitiesExport {

  def annomeseDefiniton (windowsTime:String,timeZone:String) : Int = {
    var mesi = 3
    try{
      mesi = windowsTime.toInt
    }catch {
      case e: Exception => { mesi = 3}
    }
    var delay = mesi+1
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    cal.add(Calendar.MONTH, -delay)
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    annomese.toInt
  }

  def annomeseToRemoveDefiniton (windowsTime:String,timeZone:String) : Int = {
    var mesi = 3
    try{
      mesi = windowsTime.toInt
    }catch {
      case e: Exception => { mesi = 3}
    }
    var delay = mesi
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    cal.add(Calendar.MONTH, -delay)
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    annomese.toInt
  }

  def generatePastMonths(start: Int,windowsTime:String ,monthsBack: Int = 36, minGroupSize: Int = 6): List[List[Int]] = {
    val startYear = start / 100
    val startMonth = start % 100
    val startYearMonth = YearMonth.of(startYear, startMonth)
    val windowsTimeInt = windowsTime.toInt
    val limitBackInTime = monthsBack - 1
    val limitBackInTimeLoop = monthsBack - 2 - windowsTimeInt
    val pastMonths = (0 to limitBackInTimeLoop).map { i =>
      val ym = startYearMonth.minusMonths(i)
      ym.getYear * 100 + ym.getMonthValue
    }.toList

    pastMonths.grouped(math.max(minGroupSize, pastMonths.length / (limitBackInTime / minGroupSize))).toList
  }

  def convertUtcToLong(timeZone: String): Long = {

    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone)) // Get instance with given time zone
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone)) // Ensure formatter uses the same time zone

    dateFormat.format(cal.getTime).toLong // Convert formatted string to Long

  }

  def get34thMonth(start: Int): Int = {
    val startYear = start / 100
    val startMonth = start % 100
    val startYearMonth = java.time.YearMonth.of(startYear, startMonth)

    val targetMonth = startYearMonth.minusMonths(33) // 34th element means start - 33 months

    targetMonth.getYear * 100 + targetMonth.getMonthValue
  }

  def get37thMonthAgo(): Int = {
    val currentYearMonth = java.time.YearMonth.now()
    val targetMonth = currentYearMonth.minusMonths(36)

    targetMonth.getYear * 100 + targetMonth.getMonthValue
  }


  def getDescrTipoMisura(tp_misu: String): String = {
    val tp_mis = if (tp_misu != null && tp_misu.startsWith("SW_")) tp_misu.replaceFirst("SW_", "") else tp_misu

    val l_tipo_misure = List(
      List("PDO", "PNO", "PDO2G", "PNO2G", "Lettura Periodica"),
      List("VNO", "VNO2G", "Lettura Voltura"),
      List("RFO", "RFO2G", "RNO", "RNO2G", "Lettura di Rettifica"),
      List("RNV", "RNV2G", "Lettura di Rettifica Voltura"),
      List("TGL", "TML", "Lettura Periodica"),
      List("VTG6", "Lettura Voltura"),
      List("RGL", "RML", "Lettura di Rettifica"),
      List("RMV", "Lettura di Rettifica Voltura"),
      List("TAL", "TAV", "Autoletture")
    )

    l_tipo_misure.collectFirst {
      case el if el.init.exists(_.equalsIgnoreCase(tp_mis)) => el.last
    }.getOrElse(tp_mis)
  }



  def forceStructFieldsToAppear(df: DataFrame, structColName: String): DataFrame = {
    val fields = df.schema(structColName).dataType.asInstanceOf[StructType].fieldNames
    df.withColumn(structColName, struct(fields.map(f => col(s"$structColName.$f").as(f)): _*))
  }

  def dropFullyNullStruct(df: DataFrame, structColName: String): DataFrame = {
    val fields = df.schema(structColName).dataType.asInstanceOf[StructType].fieldNames
    val allNullCheck = fields.map(f => col(s"$structColName.$f").isNull).reduce(_ && _)
    df.withColumn(structColName, when(allNullCheck, lit(null)).otherwise(col(structColName)))
  }



}
