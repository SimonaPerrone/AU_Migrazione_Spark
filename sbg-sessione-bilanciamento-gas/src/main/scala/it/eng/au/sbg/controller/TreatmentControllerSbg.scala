package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.model.agg.{MonthTreatment, PdrWithMonthTreatmentYSBG}
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasVarTrattamentoP
import it.eng.au.aggiustamentoGas.utility.constants.{TreatmentCalcMode, TreatmentConstant}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.dateTimeOrdering
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.joda.time.Months
import org.joda.time.format.DateTimeFormat

class TreatmentControllerSbg {
  def calc(rcuGasVarTrattamentoP: RDD[RcuGasVarTrattamentoP], startFlowDate: String, endFlowDate: String): RDD[MonthTreatment] = {
    val formatDate = "yyyyMM"
    val computationStartDate = DateTimeFormat.forPattern(formatDate)
      .parseDateTime(startFlowDate)
      .dayOfMonth().withMinimumValue()
    val computationEndDate = DateTimeFormat.forPattern(formatDate)
      .parseDateTime(endFlowDate)
      .dayOfMonth().withMaximumValue()

    val numMonths = Months.monthsBetween(computationStartDate, computationEndDate).getMonths

    val listMonths = (0 to numMonths).toList.map(x => computationStartDate.plusMonths(x))
    //    val listMonthsString = listMonths.map(_.toString(formatDate))

    rcuGasVarTrattamentoP.flatMap(treatment => {
      listMonths.flatMap(month => {
        val listMonthTreatment: List[((String, String), Option[RcuGasVarTrattamentoP])] =
          if (DateUtility.dateSegmentsIntersects(month, month.dayOfMonth().withMaximumValue(), treatment.dataInizio, treatment.dataFine))
            List(((treatment.codicePdr, month.toString(formatDate)), Some(treatment)))
          else List(((treatment.codicePdr, month.toString(formatDate)), None))
        listMonthTreatment
      })
    })
    .groupByKey.cache()
    .map({ case ((pdr, month), treatments) =>
        if (treatments.exists(_.isDefined)) {
          val serchMonthTreatment = treatments.filter(_.isDefined).map(_.get).minBy(treatment => treatment.dataInizio)(dateTimeOrdering)
          MonthTreatment(
            pdr = pdr,
            month = month,
            treatment = serchMonthTreatment.tTrattamentoSettlement.toString,
            calcmode = TreatmentCalcMode.rcugas.toString,
            autofilled = false
          )
        } else {
          MonthTreatment(
            pdr = pdr,
            month = month,
            treatment = TreatmentConstant.N,
            calcmode = TreatmentCalcMode.rcugas.toString,
            autofilled = true
          )
        }
      })

  }

  def generateMonthTreatmentOnPdrWithTreatmentY(ds: Dataset[PdrWithMonthTreatmentYSBG], startDate: String): RDD[MonthTreatment] = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    ds
      .map(pdr => {
        MonthTreatment(
          pdr = pdr.pdr,
          month = startDate,
          treatment = "Y",
          calcmode = TreatmentCalcMode.rcugas.toString,
          autofilled = false
        )
      })
      .rdd

  }
}


