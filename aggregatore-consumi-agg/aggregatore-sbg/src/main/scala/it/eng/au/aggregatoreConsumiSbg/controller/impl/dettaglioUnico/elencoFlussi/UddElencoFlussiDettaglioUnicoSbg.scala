package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioUnicoSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioPdrG.IdDettaglioGSbg.keyFields
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object UddElencoFlussiDettaglioUnicoSbg extends ElencoFlussiDettaglioUnicoSbg {
  override val keyPiva1: String = DettaglioUnicoSchema.piva_distr.toString
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd.toString
  //CR 04/08/2022: differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd, but path and zip names should be with pivaUdd as before
  override val mainPiva: String = keyFields.last
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> DettaglioUnicoSchema.pdr.toString,
    DailyConsumptionAggSchema.value.toString -> DettaglioUnicoSchema.Prelievo.toString,
    DailyConsumptionAggSchema.treatment.toString -> DettaglioUnicoSchema.Trattamento.toString,
    DailyConsumptionAggSchema.leftMeasureLocalFile.toString -> DettaglioUnicoSchema.Nome_file.toString,
    DailyConsumptionAggSchema.session.toString -> DettaglioUnicoSchema.Sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> DettaglioUnicoSchema.Annomese.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> DettaglioUnicoSchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> DettaglioUnicoSchema.piva_distr.toString
  )
  override val baseNumber: String = "1"

  override def getCsvFields(dfAggregato: DataFrame): List[String] = dfAggregato.columns.toList.diff(List(keyPiva1, keyPiva2))

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdd).isNotNull and col(DailyConsumptionAggSchema.pivaDistr).isNotNull

  //CR 04/08/2022: differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd, but path and zip names should be with pivaUdd as before
  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva2}_${piva1}_${sessionName}_${annomese}_ElencoFlussi_${timestamp}_${counterCsv}.csv"
  }
}
