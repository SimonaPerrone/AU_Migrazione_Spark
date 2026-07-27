package it.eng.au.portale_consumi_ee.common.utility.functions

import org.apache.spark.sql.functions.{current_date, date_sub}

object costants {
  val DOT = "."
  // Current date
  val currentDate = current_date()

  // Date 1126 days before the current date
  val dateMinus1126 = date_sub(currentDate, 1126)

  /***
   * Converte in stringa gli elementi all'interno dell'array. Usato per il print di case class complesse
   */
  def attributeToString(attributeList: List[Any]): String = {
    var str = ""
    for ((f, i) <- attributeList.zipWithIndex) {
      str = str + "\n" + f.toString
      if (i+1 < attributeList.length){
        str = str + ","
      }
    }
    str
  }
}
