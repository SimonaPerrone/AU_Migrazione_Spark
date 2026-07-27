package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioUnicoSchema}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.immutable.ListMap

object UdbPdrDettaglioUnicoSbg extends PdrDettaglioUnicoSbg {
  override val baseNumber: String = "5"
  //  override val keyFields: List[String] = List(DettaglioUnicoSchema.Piva_Udb, DettaglioUnicoSchema.piva_udd)
  override val keyPiva1: String = DettaglioUnicoSchema.Piva_Udb
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd
  override val mainPiva: String = keyPiva1
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
    DailyConsumptionAggSchema.pivaUdd.toString -> DettaglioUnicoSchema.piva_udd.toString
  )

  def getCsvFields(dfAggregato: DataFrame): List[String] = dfAggregato.columns.toList.diff(List(DettaglioUnicoSchema.piva_udd.toString))

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdd).isNotNull and col(DailyConsumptionAggSchema.pivaUdb).isNotNull

}
