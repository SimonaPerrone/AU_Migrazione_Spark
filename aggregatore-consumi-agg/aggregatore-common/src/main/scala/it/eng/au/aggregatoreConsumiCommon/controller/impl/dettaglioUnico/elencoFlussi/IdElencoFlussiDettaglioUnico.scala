package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioUnicoSchema}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.immutable.ListMap

object IdElencoFlussiDettaglioUnico extends ElencoFlussiDettaglioUnico {
  override val keyPiva1: String = DettaglioUnicoSchema.piva_distr
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd
  override val mainPiva: String = keyPiva1
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

  override val baseNumber: String = "4"

  override def getCsvFields(dfAggregato: DataFrame): List[String] = dfAggregato.columns.toList.diff(List(keyPiva1, keyPiva2))

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdd).isNotNull and col(DailyConsumptionAggSchema.pivaDistr).isNotNull

}
