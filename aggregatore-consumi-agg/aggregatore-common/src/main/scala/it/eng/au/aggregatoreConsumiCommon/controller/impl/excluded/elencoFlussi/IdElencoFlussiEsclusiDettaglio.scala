package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, EsclusiElencoFlussiOutputSchema, EsclusiOutputSchema, ValidatedFlowsAggSchema}

import scala.collection.immutable.ListMap

object IdElencoFlussiEsclusiDettaglio extends ElencoFlussiDettaglioEsclusi {
  override val baseNumber: String = "4"
  override val keyPiva1: String = EsclusiOutputSchema.piva_distr
  override val keyPiva2: String = EsclusiOutputSchema.piva_udd
  override val mainPiva: String = keyPiva1

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    ValidatedFlowsAggSchema.pdr.toString -> EsclusiElencoFlussiOutputSchema.pdr.toString,
    ValidatedFlowsAggSchema.localFile.toString -> EsclusiElencoFlussiOutputSchema.nomefile.toString,
    DailyConsumptionAggSchema.session.toString -> EsclusiElencoFlussiOutputSchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> EsclusiElencoFlussiOutputSchema.annoMese.toString,
    ValidatedFlowsAggSchema.measure.toString -> EsclusiElencoFlussiOutputSchema.let_tot_prel.toString,
    ValidatedFlowsAggSchema.converted.toString -> EsclusiElencoFlussiOutputSchema.let_tot_conv.toString,
    ValidatedFlowsAggSchema.date.toString -> EsclusiElencoFlussiOutputSchema.data_lettura.toString,
    ValidatedFlowsAggSchema.readType.toString -> EsclusiElencoFlussiOutputSchema.tipo_lettura.toString,
    ValidatedFlowsAggSchema.serialNumberMis.toString -> EsclusiElencoFlussiOutputSchema.matr_mis.toString,
    ValidatedFlowsAggSchema.serialNumberConv.toString -> EsclusiElencoFlussiOutputSchema.matr_conv.toString,
    ValidatedFlowsAggSchema.nCoeffCor.toString -> EsclusiElencoFlussiOutputSchema.coeff_cor.toString,
    ValidatedFlowsAggSchema.motivation.toString -> EsclusiElencoFlussiOutputSchema.mot_ret_lett.toString,
    ValidatedFlowsAggSchema.cauIntMis.toString -> EsclusiElencoFlussiOutputSchema.cau_int_mis.toString,
    ValidatedFlowsAggSchema.cauIntCorr.toString -> EsclusiElencoFlussiOutputSchema.cau_int_cor.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(EsclusiOutputSchema.piva_distr))
  override val header: String = ""
}
