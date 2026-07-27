package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.controller.ConsumptionController
import it.eng.au.aggiustamentoGas.model.agg.{Consumption, ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.Im1Igmg
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rgl, Tgl}
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility

class ConsumptionControllerSbg extends ConsumptionController {

  //case if distance of measure is 1 day
  override def specificSbgConditionFormula2(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Boolean = {
    startMeasure.monthTreatment.isDefined &&
      endMeasure.monthTreatment.isDefined &&
      (startMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) ||
        startMeasure.monthTreatment.get.treatment.equals(Treatment.M.toString)) &&
      (endMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) ||
        endMeasure.monthTreatment.get.treatment.equals(Treatment.M.toString)) &&
      DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get) == 1
  }

  override def specificSbgOutputFormula2(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, externalDailyInfo: ExternalDailyInfo, pprofMap: Map[(String, String, Int), Double]): List[Consumption] = {
    List(computeDifferenceOnlyConsumptionWithSerialCheck(startMeasure, endMeasure)).map(addExternalInfoToConsumption(_, externalDailyInfo, pprofMap))
  }

  override def specificConditionFormula2ShouldNotFormula4(flow1: FlowWithInfo, flow2: FlowWithInfo): Boolean = {
//
//val (sx, dx) = (flow1.flow, flow2.flow) match {
//  case (im1IgmgSx: Im1Igmg, flowDx) => (im1IgmgSx.sameDayFlow.getOrElse(im1IgmgSx), flowDx)
//  case (flowSx, im1IgmgDx: Im1Igmg) => (flowSx, im1IgmgDx.sameDayFlow.getOrElse(im1IgmgDx))
//}
//
//    (sx, dx) match {
    (flow1.flow, flow2.flow) match {
      case (_: Tgl, _) => true
      case (_: Rgl, _) => true
      case (_, _: Tgl) => true
      case (_, _: Rgl) => true
      case (_, _) => false
    }
  }
}
