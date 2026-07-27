package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.controller.TreatmentController._
import it.eng.au.aggiustamentoGas.model.agg.MonthTreatment
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasVarTrattamentoP
import it.eng.au.aggiustamentoGas.utility.constants.{Treatment, TreatmentCalcMode}
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.dateTimeOrdering
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Months}

/** Controller per il caclolo del trattamento mensile */
class TreatmentController {
  /**
   * Calcola il trattamento mensile per i flussi contenuti in [[measures]].
   * @param measures RDD contenente le misure
   * @param startDate estremo sinistro di lettura della misure
   * @param endDate estremo destro di lettura della misure
   * @param treatmentCalcMode "infered" (estratto dalle misure) o "rcugas" (estratto da rcugas)
   * @param rcuGasTrattamento tabella su rcugas contenente il trattamento
   * @return un RDD con il trattamento mensile per ogni PdR e ogni mese considerati
   */
  def calc(measures: RDD[Flow], startDate: String, endDate: String, treatmentCalcMode: TreatmentCalcMode.Value, rcuGasTrattamento: RDD[RcuGasVarTrattamentoP]): RDD[MonthTreatment] = {
    val dateParser = DateTimeFormat.forPattern("yyyyMM")

    val startDatetime = dateParser.parseDateTime(startDate)
    val endDatetime = dateParser.parseDateTime(endDate).dayOfMonth().withMaximumValue()

    val treatmentMeasures = measures.filter(flow =>
      DateUtility.isBetween(flow.date.get, startDatetime, endDatetime)
    )

    val monthTreatment = treatmentCalcMode match {
      case TreatmentCalcMode.infered => {
        val treatmentRDD = calcMonthTreatment(treatmentMeasures)
        fillEmptyMonthTreatment(treatmentRDD, startDatetime, endDatetime)
      }
      case TreatmentCalcMode.rcugas => getRcuTreatmentWithoutMeasures(rcuGasTrattamento, startDate, endDate).filter(_._2 != Treatment.N)
    }

    monthTreatment.map({ case ((pdr, month), treatment, autoFilled) =>
      MonthTreatment(
        pdr = pdr,
        month = month,
        treatment = treatment.toString,
        calcmode = treatmentCalcMode.toString,
        autofilled = autoFilled
      )
    })
  }

  /**
   * Calcola il trattamento dalle misure utilizzando le logiche contenute in [[rules]].
   * @param measures RDD of measures
   * @return RDD with pdr, month and its treatment calculated with AU rules
   */
  def calcMonthTreatment(measures: RDD[Flow]): RDD[((String, String), Treatment.Value)] = {
    measures.filter(f =>
      (f.isInstanceOf[Rml] && f.asInstanceOf[Rml].tipoRettifica != Some("T")) ||
        (f.isInstanceOf[Tml] && f.readType == Some('E')) ||
        (!f.isInstanceOf[Tml] && !f.isInstanceOf[Rml])
    )
      .keyBy(f => (f.pdr, f.date.get.toString("yyyyMM")))
      .mapValues({ flow =>
        val (treatment, index) = rules.find({ case (rule, _) => rule(flow) }).map(_._2).getOrElse((Treatment.N, 1000))
        val dayOfMonth = flow.date.get.getDayOfMonth
        (treatment, index, dayOfMonth)
      }).groupByKey()
      .mapValues(f => {
        f.minBy({ case (treatment, index, dayOfMonth) =>
          if (treatment == Treatment.N) (dayOfMonth + 31, index) else (dayOfMonth, index)
        })._1
      })
  }

  /**
   * Calcola il trattamento da rcugas: dato un certo PdR, per ogni mese viene preso il primo trattamento non nullo disponibile; se non ve n'è, viene inserito il valore [[Treatment.N]].
   * @param rcugasTrattamento tabella contenente i trattamenti per ogni PdR, con le rispettive date di inizio e di fine
   * @param startDate estremo sinistro di lettura delle misure
   * @param endDate estremo destro di lettura delle misure
   * @return un key-value RDD, dove la chiave è (pdr, mese) e il valore associato è il trattamento assieme a un booelano che indica se il trattamento è stato calcolato oppure inserito automaticamente.
   */
  def getRcuTreatmentWithoutMeasures(rcugasTrattamento: RDD[RcuGasVarTrattamentoP], startDate: String, endDate: String): RDD[((String, String), Treatment.Value, Boolean)] = {
    val formatDate = "yyyyMM"
    val computationStartDate = DateTimeFormat.forPattern(formatDate)
      .parseDateTime(startDate)
      .dayOfMonth().withMinimumValue()
    val computationEndDate = DateTimeFormat.forPattern(formatDate)
      .parseDateTime(endDate)
      .dayOfMonth().withMaximumValue()

    val numMonths = Months.monthsBetween(computationStartDate, computationEndDate).getMonths

    val listMonths = (0 to numMonths).toList.map(x => computationStartDate.plusMonths(x))

    rcugasTrattamento.flatMap(treatment => {
      listMonths.flatMap(month => {
        val listMonthTreatment: List[((String, String), Option[RcuGasVarTrattamentoP])] =
          if (DateUtility.dateSegmentsIntersects(month, month.dayOfMonth().withMaximumValue(), treatment.dataInizio, treatment.dataFine))
            List(((treatment.codicePdr, month.toString(formatDate)), Some(treatment)))
          else List(((treatment.codicePdr, month.toString(formatDate)), None))
        listMonthTreatment
      })
    }).groupByKey.cache()
      .map({ case ((pdr, month), treatments) =>
        if (treatments.exists(_.isDefined)) {
          val serchMonthTreatment = treatments.filter(_.isDefined).map(_.get).minBy(treatment => treatment.dataInizio)(dateTimeOrdering)
          ((pdr, month), serchMonthTreatment.tTrattamentoSettlement, false)
        }
        else ((pdr, month), Treatment.N, true)
      })
  }

  @Deprecated
  def getRcuTreatment(measures: RDD[Flow], rcuGasTrattamento: RDD[RcuGasVarTrattamentoP]): RDD[((String, String), Treatment.Value)] = {
    val meas = measures.map(f => ((f.pdr, f.date.get.toString("yyyyMM")), (f.treatment, f.date.get.getDayOfMonth)))
      .reduceByKey((t1, t2) =>
        List(t1, t2).minBy({ case (treat, dayOfMonth) => (dayOfMonth, priorityTreatment(treat)) })
      ).map({ case ((pdr, month), (treat, _)) => (pdr, (month, treat)) }).groupByKey()

    val rcuTreatment = rcuGasTrattamento.map(t => (t.codicePdr, (t.dataInizio, t.dataFine, t.tTrattamentoSettlement))).groupByKey()

    meas.leftOuterJoin(rcuTreatment).flatMapValues({ case (measureTuples, optRcuTuples) =>
      measureTuples.map({ case (month, treatment) =>
        if (optRcuTuples.isEmpty) (month, treatment) else {
          val dateParser = DateTimeFormat.forPattern("yyyyMM")
          val firstDayOfMonth = dateParser.parseDateTime(month)
          val lastDayOfMonth = firstDayOfMonth.dayOfMonth().withMaximumValue()

          val treatRcuList = optRcuTuples.get.filter({ case (startDate, endDate, _) =>
            DateUtility.dateSegmentsIntersects(firstDayOfMonth, lastDayOfMonth, startDate, endDate)
          })

          val treatRcu =
            if (treatRcuList.nonEmpty) treatRcuList.minBy(_._1)(DateUtility.dateTimeOrdering)._3
            else Treatment.N

          (month, treatRcu)
        }
      })
    }).map({ case (pdr, (month, treatment)) => ((pdr, month), treatment) })

  }

  /**
   * Se il trattamento viene calcolato dalle misure, nei mesi senza trattamento viene inserito un valore seguendo le logiche definite in [[fillMap]].
   * @param monthTreatment RDD with pdr, month and treatment from measures
   * @return RDD with pdr, filled empty month and treatment
   */
  def fillEmptyMonthTreatment(monthTreatment: RDD[((String, String), Treatment.Value)], startDatetime: DateTime, endDatetime: DateTime): RDD[((String, String), Treatment.Value, Boolean)] = {
    monthTreatment.map({ case ((pdr, month), treatment) => (pdr, (month, treatment)) }).groupByKey().flatMapValues(list => {
      val monthsWithtreat = calcMonthsPeriod(list, startDatetime, endDatetime).map({ case (month, treatment) => (month, treatment, false) })

      val flowMonthOrdered = monthsWithtreat.toList.sortBy({ case (month, treatment, autoFilled) => month })

      val leftOrder = fillMap(flowMonthOrdered)
      val rightOrder = fillMap(leftOrder.reverse)

      rightOrder
    }).map({ case (pdr, (month, treatment, autoFilled)) => ((pdr, month), treatment, autoFilled) })
  }
}

object TreatmentController {
  /**
   * list ordered of rules with its treatment
   */
  def rules: List[(Flow => Boolean, (Treatment.Value, Int))] = List(
    ((f: Flow) => f.isInstanceOf[Rgl] && Set(1, 2, 3, 4, 5, 7).contains(f.asInstanceOf[Rgl].motivation.getOrElse(-1)), Treatment.G),
    ((f: Flow) => f.isInstanceOf[Rml] && Set(1, 2, 3, 4, 5, 6).contains(f.asInstanceOf[Rml].motivation.getOrElse(-1)) && f.asInstanceOf[Rml].freqLet == Some(4),
      Treatment.M),
    ((f: Flow) => f.isInstanceOf[Rml] && Set(1, 2, 3, 4, 5, 6).contains(f.asInstanceOf[Rml].motivation.getOrElse(-1)) &&
      Set(1, 2, 3, 5, 6, 7).contains(f.asInstanceOf[Rml].freqLet.getOrElse(-1)), Treatment.Y),

    ((f: Flow) => f.isInstanceOf[Tgl] && Set('E', 'S').contains(f.asInstanceOf[Tgl].readType.getOrElse('-')) &&
      (f.asInstanceOf[Tgl].isValid.getOrElse("") == "SI" || f.ammissibilita.isDefined), Treatment.G),
    ((f: Flow) => f.isInstanceOf[Tml] && Set('E').contains(f.asInstanceOf[Tml].readType.getOrElse('-')) && f.asInstanceOf[Tml].freqLet == Some(4) &&
      (f.asInstanceOf[Tml].isValid.getOrElse("") == "SI" || f.ammissibilita.isDefined), Treatment.M),
    ((f: Flow) => f.isInstanceOf[Tml] && Set('E').contains(f.asInstanceOf[Tml].readType.getOrElse('-')) && Set(1, 2, 3, 5, 6, 7).contains(f.asInstanceOf[Tml].freqLet.getOrElse(-1)) &&
      (f.asInstanceOf[Tml].isValid.getOrElse("") == "SI" || f.ammissibilita.isDefined), Treatment.Y)
  ).zipWithIndex.map({ case ((rule, treatment), index) => (rule, (treatment, index)) })

  def priorityTreatment: Map[Treatment.Value, Int] = List(Treatment.G, Treatment.M, Treatment.Y, Treatment.N).zipWithIndex.toMap

  /**
   *
   * @param monthTreatMeasures iterable with month and treatment from measures
   * @param startDate          start date input
   * @param endDate            end date input
   * @return sequence of month and treatment filled with empty months
   */
  def calcMonthsPeriod(monthTreatMeasures: Iterable[(String, Treatment.Value)], startDate: DateTime, endDate: DateTime): Seq[(String, Treatment.Value)] = {
    var currMonth = startDate
    var months: List[DateTime] = List()
    while (currMonth.isBefore(endDate) || currMonth == endDate) {
      months = months :+ currMonth
      currMonth = currMonth.plusMonths(1)
    }

    val map = monthTreatMeasures.toMap

    val monthsWithtreat = months.map(m => {
      val monthString = m.toString("yyyyMM")
      val treat = map.getOrElse(monthString, Treatment.N)
      (monthString, treat)
    })
    monthsWithtreat
  }

  /**
   *
   * @param flowMonthOrdered list with month and treatment
   * @return list with month and right treatment using AU rules
   */
  def fillMap(flowMonthOrdered: List[(String, Treatment.Value, Boolean)]): List[(String, Treatment.Value, Boolean)] = {
    var previousTreatment: Option[Treatment.Value] = None
    for (m <- flowMonthOrdered.zipWithIndex) yield {
      val ((month, treatment, autoFilled), index) = m
      val (correctTreat, isAutoFilled) =
        if (treatment == Treatment.N && previousTreatment.isDefined && Set(Treatment.Y, Treatment.M).contains(previousTreatment.get))
          (previousTreatment.get, true)
        else
          (treatment, autoFilled)
      previousTreatment = Some(correctTreat)
      (month, correctTreat, isAutoFilled)
    }
  }
}
