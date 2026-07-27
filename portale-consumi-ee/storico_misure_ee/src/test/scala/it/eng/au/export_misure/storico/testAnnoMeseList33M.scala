package it.eng.au.export_misure.storico

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilities

import java.util.{Calendar, TimeZone}

class testAnnoMeseList33M extends EnvironmentSparkTest{

  val spark = EnvironmentMisure.getSpark

  import spark.implicits._

  def testAnnomeseToRemoveDefiniton():Unit ={
    val windowTimeValue = "5"
    val timeZone = "UTC"
  val resultToRemove = argumentsUtilities.annomeseToRemoveDefiniton(windowTimeValue,timeZone)

    println(resultToRemove)

  }

  def testAnnomeseLoop ()= {
    val windowTimeValue = 6

    var ym = java.time.YearMonth.now()
    var monthAgoLimit = 3
    try{
      monthAgoLimit = windowTimeValue.toInt
    }catch {
      case e: Exception => { monthAgoLimit = 3}
    }

    for (_ <- 0 until monthAgoLimit) {
      val annomese = ym.getYear * 100 + ym.getMonthValue
      println(annomese)
      ym = ym.minusMonths(1)
    }


  }

  def testremovePartition (): Unit = {

    val month37th = 202301

    val partitions =  Set(
      "annomese_riferimento=202503/cod_pod=IT/is_mis_oraria=Y",
      "annomese_riferimento=202503/cod_pod=FR/is_mis_oraria=N",
      "annomese_riferimento=202503/cod_pod=DE/is_mis_oraria=Y",
      "annomese_riferimento=202503/cod_pod=ES/is_mis_oraria=N",
      "annomese_riferimento=202503/cod_pod=GB/is_mis_oraria=Y"
    )

    if (partitions.isEmpty) {
      println(s"No partitions found for annomese_riferimento=$month37th")
    } else {
      partitions.foreach { partition =>
        // Convert partition format for DROP statement
        val partitionSpec = partition.split("/")
          .map {
            part =>
              val Array(key, value) = part.split("=")
              s"$key='$value'"
          }.mkString(", ")

        val dropPartitionQuery = s"ALTER TABLE nome_prova DROP IF EXISTS PARTITION ($partitionSpec)"
        println(s"Executing: $dropPartitionQuery")
      }

    }
  }

  def testConsultazioneLimit (): Unit =  {
    val windowTimeValue = 5

    val monthAgoLimit = argumentsUtilities.numberMonthAgoToDefine(windowTimeValue.toString)

    println(monthAgoLimit)

    var ym = java.time.YearMonth.now()
    for (_ <- 0 until monthAgoLimit) {
      val annomese = ym.getYear * 100 + ym.getMonthValue
      println(annomese)
      ym = ym.minusMonths(1)



    }

  }

  def testAnnoMeseDefinition ():Unit = {
    val windowsTime = "5"
    val timeZone = "UTC"

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

    println(annomese)
  }

}
