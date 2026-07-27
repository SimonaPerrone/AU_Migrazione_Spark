package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioUnicoSchema}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object UddPdrDettaglioUnicoSbg extends PdrDettaglioUnicoSbg {
  override val baseNumber: String = "1"
  override val keyPiva1: String = DettaglioUnicoSchema.piva_distr.toString
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd.toString
  //CR 04/08/2022: differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd, but the path and zip name should be with pivaUdd as before
  override val mainPiva: String = keyPiva2
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> DettaglioUnicoSchema.pdr.toString,
    DailyConsumptionAggSchema.value.toString -> DettaglioUnicoSchema.Prelievo.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> DettaglioUnicoSchema.Piva_Udb.toString,
    DailyConsumptionAggSchema.dtg.toString -> DettaglioUnicoSchema.Dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> DettaglioUnicoSchema.Cod_Remi.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> DettaglioUnicoSchema.Id_regione_climatica.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> DettaglioUnicoSchema.Cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> DettaglioUnicoSchema.Trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> DettaglioUnicoSchema.Tipo_cliente.toString,
    DailyConsumptionAggSchema.session.toString -> DettaglioUnicoSchema.Sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> DettaglioUnicoSchema.Annomese.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> DettaglioUnicoSchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> DettaglioUnicoSchema.piva_distr.toString
  )

  override def getCsvFields(dfAggregato: DataFrame): List[String] = dfAggregato.columns.toList.diff(List(keyPiva1, keyPiva2))

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull and col(DailyConsumptionAggSchema.pivaUdd).isNotNull

  //CR 04/08/2022: differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd,
  // but the path name should be with pivaUdd as before
  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva2}_${piva1}_${sessionName}_${annomese}_${timestamp}_${counterCsv}.csv"
  }
}
