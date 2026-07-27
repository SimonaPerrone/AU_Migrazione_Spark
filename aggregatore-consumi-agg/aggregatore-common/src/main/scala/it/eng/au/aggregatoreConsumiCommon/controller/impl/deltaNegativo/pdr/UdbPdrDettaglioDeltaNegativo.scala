package it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DeltaNegativoDettaglioSchema}
import scala.collection.immutable.ListMap

object UdbPdrDettaglioDeltaNegativo extends PdrDettaglioDeltaNegativo {
  override val baseNumber: String = "1"
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_udb
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.date.toString -> DeltaNegativoDettaglioSchema.data,
    DailyConsumptionAggSchema.pdr.toString -> DeltaNegativoDettaglioSchema.cod_pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> DeltaNegativoDettaglioSchema.piva_distr,
    DailyConsumptionAggSchema.pivaIt.toString -> DeltaNegativoDettaglioSchema.piva_it,
    DailyConsumptionAggSchema.pivaUdd.toString -> DeltaNegativoDettaglioSchema.piva_udd,
    DailyConsumptionAggSchema.pivaUdb.toString -> DeltaNegativoDettaglioSchema.piva_udb,
    DailyConsumptionAggSchema.dtg.toString -> DeltaNegativoDettaglioSchema.dtg,
    DailyConsumptionAggSchema.codRemi.toString -> DeltaNegativoDettaglioSchema.cod_remi,
    DailyConsumptionAggSchema.ca.toString -> DeltaNegativoDettaglioSchema.prel_annuo_prev,
    DailyConsumptionAggSchema.treatment.toString -> DeltaNegativoDettaglioSchema.trattamento,
    DailyConsumptionAggSchema.tipoCliente.toString -> DeltaNegativoDettaglioSchema.tipo_cliente,
    DailyConsumptionAggSchema.unitMisPrel.toString -> DeltaNegativoDettaglioSchema.un_mis_prel,
    DailyConsumptionAggSchema.annoMese.toString -> DeltaNegativoDettaglioSchema.annomese,
    DeltaNegativoDettaglioSchema.GIORN_DN.toString -> DeltaNegativoDettaglioSchema.GIORN_DN,
    DeltaNegativoDettaglioSchema.PRELIEVO_GIORNO_DN.toString -> DeltaNegativoDettaglioSchema.PRELIEVO_GIORNO_DN
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList
}
