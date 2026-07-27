package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IncoerentiDettaglioSchema}

import scala.collection.immutable.ListMap

object UdbPdrIncoerentiDettaglioSbg extends PdrDettaglioIncoerentiSbg {
  override val baseNumber: String = "5"
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_udb)
  override val mainPiva: String = keyFields.head

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> IncoerentiDettaglioSchema.cod_pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> IncoerentiDettaglioSchema.piva_distr,
    DailyConsumptionAggSchema.pivaIt.toString -> IncoerentiDettaglioSchema.piva_it,
    DailyConsumptionAggSchema.pivaUdd.toString -> IncoerentiDettaglioSchema.piva_udd,
    DailyConsumptionAggSchema.pivaUdb.toString -> IncoerentiDettaglioSchema.piva_udb,
    DailyConsumptionAggSchema.dtg.toString -> IncoerentiDettaglioSchema.dtg,
    DailyConsumptionAggSchema.codRemi.toString -> IncoerentiDettaglioSchema.cod_remi,
    DailyConsumptionAggSchema.ca.toString -> IncoerentiDettaglioSchema.prel_annuo_prev,
    DailyConsumptionAggSchema.idRegClim.toString -> IncoerentiDettaglioSchema.id_reg_clim,
    DailyConsumptionAggSchema.codProfStd.toString -> IncoerentiDettaglioSchema.cod_prof_prel_std,
    DailyConsumptionAggSchema.treatment.toString -> IncoerentiDettaglioSchema.trattamento,
    DailyConsumptionAggSchema.tipoCliente.toString -> IncoerentiDettaglioSchema.tipo_cliente,
    DailyConsumptionAggSchema.unitMisPrel.toString -> IncoerentiDettaglioSchema.un_mis_prel,
    DailyConsumptionAggSchema.classeMisuratore.toString -> IncoerentiDettaglioSchema.classe_gruppo_mis,
    prelievoAggregato -> IncoerentiDettaglioSchema.prelievo_aggregato,
    anomalousDays -> IncoerentiDettaglioSchema.giorno_sterilizzato
  )

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList
}
