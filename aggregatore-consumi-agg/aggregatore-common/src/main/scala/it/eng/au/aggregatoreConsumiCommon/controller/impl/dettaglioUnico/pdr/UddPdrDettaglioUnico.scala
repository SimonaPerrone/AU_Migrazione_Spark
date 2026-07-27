package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioUnicoSchema}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.immutable.ListMap

object UddPdrDettaglioUnico extends PdrDettaglioUnico {
  override val baseNumber: String = "1"
  //  override val keyFields: List[String] = List(DettaglioUnicoSchema.piva_udd, DettaglioUnicoSchema.piva_distr)
  override val keyPiva1: String = DettaglioUnicoSchema.piva_udd
  override val keyPiva2: String = DettaglioUnicoSchema.piva_distr
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
    DailyConsumptionAggSchema.pivaUdd.toString -> DettaglioUnicoSchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> DettaglioUnicoSchema.piva_distr.toString
  )

  override def getCsvFields(dfAggregato: DataFrame): List[String] = dfAggregato.columns.toList.diff(List(keyPiva1, keyPiva2))

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull and col(DailyConsumptionAggSchema.pivaUdd).isNotNull

}
