package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IncoerentiDettaglioSchema, ValidatedFlowsAggSchema}

import scala.collection.immutable.ListMap

object IdElencoFlussiIncoerentiDettaglio extends ElencoFlussiDettaglioIncoerenti {
  override val baseNumber: String = "4"
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_distr)
  override val mainPiva: String = keyFields.head

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> IncoerentiDettaglioSchema.pdr.toString,
    ValidatedFlowsAggSchema.localFile.toString -> IncoerentiDettaglioSchema.nomefile.toString,
    DailyConsumptionAggSchema.session.toString -> IncoerentiDettaglioSchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> IncoerentiDettaglioSchema.annomese.toString,
    ValidatedFlowsAggSchema.measure.toString -> IncoerentiDettaglioSchema.let_tot_prel.toString,
    ValidatedFlowsAggSchema.converted.toString -> IncoerentiDettaglioSchema.let_tot_conv.toString,
    ValidatedFlowsAggSchema.date.toString -> IncoerentiDettaglioSchema.data_lettura.toString,
    ValidatedFlowsAggSchema.readType.toString -> IncoerentiDettaglioSchema.tipo_lettura.toString,
    ValidatedFlowsAggSchema.serialNumberMis.toString -> IncoerentiDettaglioSchema.matr_mis.toString,
    ValidatedFlowsAggSchema.serialNumberConv.toString -> IncoerentiDettaglioSchema.matr_conv.toString,
    ValidatedFlowsAggSchema.nCoeffCor.toString -> IncoerentiDettaglioSchema.coeff_cor.toString,
    ValidatedFlowsAggSchema.motivation.toString -> IncoerentiDettaglioSchema.mot_ret_lett.toString,
    ValidatedFlowsAggSchema.cauIntMis.toString -> IncoerentiDettaglioSchema.cau_int_mis.toString,
    ValidatedFlowsAggSchema.cauIntCorr.toString -> IncoerentiDettaglioSchema.cau_int_cor.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(IncoerentiDettaglioSchema.piva_distr))
}
