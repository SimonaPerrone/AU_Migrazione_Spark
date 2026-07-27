package it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, GiroContatoreDettaglioSchema}
import scala.collection.immutable.ListMap

object UddPdrDettaglioGiroContatore extends PdrDettaglioGiroContatore {
  override val baseNumber: String = "4"
  override val keyPiva1: String = GiroContatoreDettaglioSchema.piva_udd
  override val keyPiva2: String = GiroContatoreDettaglioSchema.piva_distr
  override val mainPiva: String = keyPiva1

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.date.toString -> GiroContatoreDettaglioSchema.data,
    DailyConsumptionAggSchema.pdr.toString -> GiroContatoreDettaglioSchema.cod_pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> GiroContatoreDettaglioSchema.piva_distr,
    DailyConsumptionAggSchema.pivaIt.toString -> GiroContatoreDettaglioSchema.piva_it,
    DailyConsumptionAggSchema.pivaUdd.toString -> GiroContatoreDettaglioSchema.piva_udd,
    DailyConsumptionAggSchema.pivaUdb.toString -> GiroContatoreDettaglioSchema.piva_udb,
    DailyConsumptionAggSchema.dtg.toString -> GiroContatoreDettaglioSchema.dtg,
    DailyConsumptionAggSchema.codRemi.toString -> GiroContatoreDettaglioSchema.cod_remi,
    DailyConsumptionAggSchema.ca.toString -> GiroContatoreDettaglioSchema.prel_annuo_prev,
    DailyConsumptionAggSchema.treatment.toString -> GiroContatoreDettaglioSchema.trattamento,
    DailyConsumptionAggSchema.tipoCliente.toString -> GiroContatoreDettaglioSchema.tipo_cliente,
    DailyConsumptionAggSchema.unitMisPrel.toString -> GiroContatoreDettaglioSchema.un_mis_prel,
    DailyConsumptionAggSchema.annoMese.toString -> GiroContatoreDettaglioSchema.annomese,
    GiroContatoreDettaglioSchema.GIORN_GC.toString -> GiroContatoreDettaglioSchema.GIORN_GC,
    GiroContatoreDettaglioSchema.PRELIEVO_GIORNO_GC.toString -> GiroContatoreDettaglioSchema.PRELIEVO_GIORNO_GC
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList
}
