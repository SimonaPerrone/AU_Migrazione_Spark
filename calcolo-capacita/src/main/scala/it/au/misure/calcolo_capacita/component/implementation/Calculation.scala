package it.au.misure.calcolo_capacita.component.implementation


import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.joda.time.{LocalDate, Months}

object Calculation {

  /**
   *
   * @param args
   * @return
   */
  def calculateRange(dataCalc: LocalDate, y: Int): Tuple2[String, String] = {

    val deltaRigth = dataCalc.withDayOfMonth(1).minusDays(1)
    val deltaLeft = deltaRigth.minusDays(y).plusDays(1)

    Tuple2(deltaLeft.toString("yyyy-MM-dd"), deltaRigth.toString("yyyy-MM-dd"))
  }

  def calculateRangeDb(dataCalc: LocalDate, y: Int): Seq[String] = {

    val deltaRigth = dataCalc.withDayOfMonth(1).minusDays(1)
    val deltaLeft = deltaRigth.minusDays(y).plusDays(1)
    val monthsBetween = Months.monthsBetween(deltaLeft.dayOfMonth().withMinimumValue(), deltaRigth.dayOfMonth().withMaximumValue()).getMonths

    Seq.tabulate(monthsBetween + 1)(n => deltaLeft.plusMonths(n).toString("yyyyMM"))
  }


  def calculateDataInizio()(implicit args: Args): LocalDate = {
    args.dataCalc.plusMonths(1).withDayOfMonth(1)
  }

  def calculateAnnoTermico(dDataInizio: LocalDate): Int = {
    val monthOfYearDInizio = dDataInizio.getMonthOfYear
    val yearOfYearDInizio = dDataInizio.getYear
    val anno = if (monthOfYearDInizio == 10 || monthOfYearDInizio == 11 || monthOfYearDInizio == 12) yearOfYearDInizio + 1
    else yearOfYearDInizio
    anno
  }
}
