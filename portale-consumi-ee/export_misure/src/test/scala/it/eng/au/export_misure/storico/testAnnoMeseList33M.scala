package it.eng.au.export_misure.storico

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure._
import it.eng.au.portale_consumi_ee.schema.misure.{misureMensiliCSchema, misureOrarieCSchema}
import it.eng.au.portale_consumi_ee.trasformations.{stagePhaseTrasformation, stagePhaseTrasformationDifferentAprroach}
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.time.{LocalDate, YearMonth}
import java.time.format.DateTimeFormatter

class testAnnoMeseList33M extends EnvironmentSparkTest{

  val spark = EnvironmentMisure.getSpark

  import spark.implicits._

  def testSetDataList33m (): Unit = {

    val startValue = 202412
    val windowTimeValue = "5"
    val result = argumentsUtilitiesExport.generatePastMonths(startValue,windowTimeValue)

    result.foreach(group => println(group.mkString(", ")))

  }

  def testget34thMonth():Unit = {

    val startValue = 202412
    val result = argumentsUtilitiesExport.get34thMonth(startValue)

    println(result)

  }

  def testget37thMonth():Unit = {

    val result = argumentsUtilitiesExport.get37thMonthAgo()

    println(result)

  }

  def testannomeseToRemoveDefiniton():Unit = {

    val windowTimeValue = "5"
    val timeZone = "UTC"
    val result = argumentsUtilitiesExport.annomeseToRemoveDefiniton(windowTimeValue,timeZone)

    println(result)

  }

  def testFromTimeZoneToLong(): Unit = {
    val startValue = "UTC"
    val result = argumentsUtilitiesExport.convertUtcToLong(startValue)

    println(result)
  }

  def testAnnomeseToRemoveDefiniton():Unit ={
    val windowTimeValue = "5"
    val timeZone = "UTC"
  val resultToRemove = argumentsUtilitiesExport.annomeseToRemoveDefiniton(windowTimeValue,timeZone)

    println(resultToRemove)

    val resultFromTo = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)

    println(resultFromTo)

  }

}
