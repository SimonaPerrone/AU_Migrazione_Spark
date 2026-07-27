package it.eng.au.aggiustamentoGas.model.agg

import it.eng.au.aggiustamentoGas.utility.constants.DimensionalType
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.rcugas.{RcuGasTech, RcuGasVarConvertitore, RcuGasVarProfiloP}

case class FlowWithInfo(
                         flow: Flow,
                         monthTreatment: Option[MonthTreatment] = None,
                         rcuGasTech: Option[RcuGasTech] = None,
                         rcuGasVarConvertitore: Option[RcuGasVarConvertitore] = None,
                         rcuGasVarProfilo: Option[RcuGasVarProfiloP] = None,
                         coeff: Option[Double] = None,
                         im1IgmgCoeff: Option[Double] = None,
                         dimensionalType: Option[DimensionalType.Value] = None,
                         idRegioneClimatica: Option[Int] = None
                       )

object FlowWithInfo {
  val orderingSameDayFlows: Ordering[FlowWithInfo] = new Ordering[FlowWithInfo] {
      override def compare(x: FlowWithInfo, y: FlowWithInfo): Int = Flow.orderingSameDayFlows.compare(x.flow, y.flow)
  }
  val orderingFlowsByDateTime: Ordering[FlowWithInfo] = new Ordering[FlowWithInfo] {
    override def compare(x: FlowWithInfo, y: FlowWithInfo): Int = Flow.orderingFlowsByDateTime.compare(x.flow, y.flow)
  }
}