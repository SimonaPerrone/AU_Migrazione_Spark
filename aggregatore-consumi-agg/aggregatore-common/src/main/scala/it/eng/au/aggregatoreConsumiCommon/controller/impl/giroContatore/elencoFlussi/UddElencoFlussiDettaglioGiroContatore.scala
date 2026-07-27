package it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, GiroContatoreElencoFlussiSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.ListMap

object UddElencoFlussiDettaglioGiroContatore extends ElencoFlussiDettaglioGiroContatore {
  override val baseNumber: String = "1"
  override val keyPiva1: String = GiroContatoreElencoFlussiSchema.piva_udd
  override val keyPiva2: String = GiroContatoreElencoFlussiSchema.piva_distr
  override val mainPiva: String = keyPiva1

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    ValidatedFlowsAggSchema.pdr.toString -> GiroContatoreElencoFlussiSchema.pdr.toString,
    ValidatedFlowsAggSchema.localFile.toString -> GiroContatoreElencoFlussiSchema.nomefile.toString,
    DailyConsumptionAggSchema.session.toString -> GiroContatoreElencoFlussiSchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> GiroContatoreElencoFlussiSchema.annomese.toString,
    ValidatedFlowsAggSchema.measure.toString -> GiroContatoreElencoFlussiSchema.let_tot_prel.toString,
    ValidatedFlowsAggSchema.converted.toString -> GiroContatoreElencoFlussiSchema.let_tot_conv.toString,
    ValidatedFlowsAggSchema.date.toString -> GiroContatoreElencoFlussiSchema.data_lettura.toString,
    ValidatedFlowsAggSchema.readType.toString -> GiroContatoreElencoFlussiSchema.tipo_lettura.toString,
    ValidatedFlowsAggSchema.serialNumberMis.toString -> GiroContatoreElencoFlussiSchema.matr_mis.toString,
    ValidatedFlowsAggSchema.serialNumberConv.toString -> GiroContatoreElencoFlussiSchema.matr_conv.toString,
    ValidatedFlowsAggSchema.nCoeffCor.toString -> GiroContatoreElencoFlussiSchema.coeff_corr.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(keyPiva1, keyPiva2))
  override val header: String = ""

  override def getAggregato(df: DataFrame): DataFrame = Environment.sqlContext.emptyDataFrame
}
