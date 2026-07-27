package it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DeltaNegativoElencoFlussiSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object UddElencoFlussiDettaglioDeltaNegativoSbg extends ElencoFlussiDettaglioDeltaNegativoSbg {
  override val baseNumber: String = "1"
  override val keyPiva1: String = DeltaNegativoElencoFlussiSchema.piva_udd
  override val keyPiva2: String = DeltaNegativoElencoFlussiSchema.piva_distr
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


  //differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd, but path and zip names should be with pivaUdd as before
  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva2}_${piva1}_${sessionName}_${operationName}_${annomese}_FlussiDN_${timestamp}_${counterCsv}.csv"
  }
}
