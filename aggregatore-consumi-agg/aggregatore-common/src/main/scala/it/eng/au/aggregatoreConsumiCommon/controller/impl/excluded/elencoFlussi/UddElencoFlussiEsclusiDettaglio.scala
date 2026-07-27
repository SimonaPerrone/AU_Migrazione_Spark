package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, EsclusiElencoFlussiOutputSchema, EsclusiOutputSchema, ValidatedFlowsAggSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object UddElencoFlussiEsclusiDettaglio extends ElencoFlussiDettaglioEsclusi  {
  override val baseNumber: String = "1"
  override val keyPiva1: String = EsclusiOutputSchema.piva_udd
  override val keyPiva2: String = EsclusiOutputSchema.piva_distr
  override val mainPiva: String = keyPiva1

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    ValidatedFlowsAggSchema.pdr.toString -> EsclusiElencoFlussiOutputSchema.pdr.toString,
    ValidatedFlowsAggSchema.localFile.toString -> EsclusiElencoFlussiOutputSchema.nomefile.toString,
    DailyConsumptionAggSchema.session.toString -> EsclusiElencoFlussiOutputSchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> EsclusiElencoFlussiOutputSchema.annoMese.toString,
    ValidatedFlowsAggSchema.measure.toString -> EsclusiElencoFlussiOutputSchema.let_tot_prel.toString,
    ValidatedFlowsAggSchema.converted.toString -> EsclusiElencoFlussiOutputSchema.let_tot_conv.toString,
    ValidatedFlowsAggSchema.date.toString -> EsclusiElencoFlussiOutputSchema.data_lettura.toString,
    ValidatedFlowsAggSchema.readType.toString -> EsclusiElencoFlussiOutputSchema.tipo_lettura.toString,
    ValidatedFlowsAggSchema.serialNumberMis.toString -> EsclusiElencoFlussiOutputSchema.matr_mis.toString,
    ValidatedFlowsAggSchema.serialNumberConv.toString -> EsclusiElencoFlussiOutputSchema.matr_conv.toString,
    ValidatedFlowsAggSchema.nCoeffCor.toString -> EsclusiElencoFlussiOutputSchema.coeff_cor.toString,
    ValidatedFlowsAggSchema.motivation.toString -> EsclusiElencoFlussiOutputSchema.mot_ret_lett.toString,
    ValidatedFlowsAggSchema.cauIntMis.toString -> EsclusiElencoFlussiOutputSchema.cau_int_mis.toString,
    ValidatedFlowsAggSchema.cauIntCorr.toString -> EsclusiElencoFlussiOutputSchema.cau_int_cor.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(EsclusiOutputSchema.piva_udd))

  override def fileSpecificFilterExpression: Column = lit(true)

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva2}_${piva1}_AGG_${operationName}_${annomese}_ElencoFlussi_${timestamp}_${counterCsv}.csv"
  }

  override val header: String = ""
}
