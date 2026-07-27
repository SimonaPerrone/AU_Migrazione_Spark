package it.eng.au.portale_consumi_ee.utility.functions

import java.util.{Calendar, TimeZone}

object argumentsUtilities {

  def annomese36MonthsAgo(): Int = {
    val now = java.time.YearMonth.now()
    val newDate = now.minusMonths(36)
    newDate.getYear * 100 + newDate.getMonthValue
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

  def numberMonthAgoToDefine(windowTimeValue: String): Int = {
    var ym = java.time.YearMonth.now()
    var monthAgoLimit = 3
    try{
      monthAgoLimit = windowTimeValue.toInt
    }catch {
      case e: Exception => { monthAgoLimit = 3}
    }

    monthAgoLimit
  }

}
