package it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DeltaNegativoDettaglioSchema}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object UddPdrDettaglioDeltaNegativoSbg extends PdrDettaglioDeltaNegativoSbg {
  override val baseNumber: String = "4"
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_udd
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_distr
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

  //CR 04/08/2022: differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd,
  // but the path name should be with pivaUdd as before
  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva2}_${piva1}_${sessionName}_${operationName}_${annomese}_${timestamp}_${counterCsv}.csv"
  }
}
