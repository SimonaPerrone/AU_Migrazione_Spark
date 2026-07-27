package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.controller.JoinInfoController
import it.eng.au.aggiustamentoGas.filter.inclusion.InclusionFilterController
import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure.{Tal, Tas, Tav}
import it.eng.au.aggiustamentoGas.utility.constants.TreatmentConstant
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat

class JoinInfoControllerSbg(private val inclusionFilters: List[InclusionFilterController] = List()) extends JoinInfoController(inclusionFilters) {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  val isFilterAutoletturaTreatmentGEnabled: Boolean = Environment.isFilterAutoletturaTreatmentGEnabled.equals("true")

  //Sbg override this, in agg filter also the stimate (readType == 'S')
  override def filterAutoletturaFlow(measureWithSuspension: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))], startDate: String, endDate: String): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {
    if (isFilterAutoletturaTreatmentGEnabled) {
      measureWithSuspension.mapValues({ case (fList, externalDailyInfo) =>
        val inputDateFormatter = DateTimeFormat.forPattern("yyyyMM")
        val startDateJoda = inputDateFormatter.parseDateTime(startDate)
        val endDateJoda = inputDateFormatter.parseDateTime(endDate).dayOfMonth().withMaximumValue()

        val treatmentIsGInPeriod = fList.exists(f => f.monthTreatment.nonEmpty &&
          DateUtility.isBetween(inputDateFormatter.parseDateTime(f.monthTreatment.get.month), startDateJoda, endDateJoda) &&
          f.monthTreatment.get.treatment == TreatmentConstant.G
        )
        if (treatmentIsGInPeriod)
          (fList.filter(f => !(f.flow.readType.contains('A') || List(classOf[Tal], classOf[Tas], classOf[Tav]).exists(_.isInstance(f.flow)))), externalDailyInfo)
        else
          (fList, externalDailyInfo)
      })
    }
    else
      measureWithSuspension
  }
}