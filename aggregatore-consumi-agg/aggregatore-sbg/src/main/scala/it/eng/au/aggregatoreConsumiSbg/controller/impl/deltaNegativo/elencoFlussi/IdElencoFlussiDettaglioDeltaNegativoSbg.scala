package it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DeltaNegativoElencoFlussiSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.ListMap

object IdElencoFlussiDettaglioDeltaNegativoSbg extends ElencoFlussiDettaglioDeltaNegativoSbg {
  override val baseNumber: String = "4"
  override val keyPiva1: String = DeltaNegativoElencoFlussiSchema.piva_distr
  override val keyPiva2: String = DeltaNegativoElencoFlussiSchema.piva_udd
  override val mainPiva: String = keyPiva1

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    ValidatedFlowsAggSchema.pdr.toString -> DeltaNegativoElencoFlussiSchema.pdr.toString,
    ValidatedFlowsAggSchema.localFile.toString -> DeltaNegativoElencoFlussiSchema.nomefile.toString,
    DailyConsumptionAggSchema.session.toString -> DeltaNegativoElencoFlussiSchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> DeltaNegativoElencoFlussiSchema.annomese.toString,
    ValidatedFlowsAggSchema.measure.toString -> DeltaNegativoElencoFlussiSchema.let_tot_prel.toString,
    ValidatedFlowsAggSchema.converted.toString -> DeltaNegativoElencoFlussiSchema.let_tot_conv.toString,
    ValidatedFlowsAggSchema.date.toString -> DeltaNegativoElencoFlussiSchema.data_lettura.toString,
    ValidatedFlowsAggSchema.readType.toString -> DeltaNegativoElencoFlussiSchema.tipo_lettura.toString,
    ValidatedFlowsAggSchema.serialNumberMis.toString -> DeltaNegativoElencoFlussiSchema.matr_mis.toString,
    ValidatedFlowsAggSchema.serialNumberConv.toString -> DeltaNegativoElencoFlussiSchema.matr_conv.toString,
    ValidatedFlowsAggSchema.nCoeffCor.toString -> DeltaNegativoElencoFlussiSchema.coeff_corr.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(keyPiva1, keyPiva2))
  override val header: String = ""
  override def getAggregato(df: DataFrame): DataFrame = Environment.sqlContext.emptyDataFrame
}
