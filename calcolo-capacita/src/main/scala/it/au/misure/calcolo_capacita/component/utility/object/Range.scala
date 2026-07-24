package it.au.misure.calcolo_capacita.component.utility.`object`

import org.joda.time.LocalDate

case class Range(var left: LocalDate, var rigth: LocalDate)

object Range {

  def apply(dataCalc: LocalDate, y: Int): Range = {
    val rigth = dataCalc.withDayOfMonth(1).minusDays(1)
    val left = rigth
      .minusDays(y)
      /*perchè consideriamo i due estremi inclusi, poichè
      * jo conta dal rigth +1, il left viene -1*/
      .plusDays(1)
    Range(left, rigth)
  }
}
