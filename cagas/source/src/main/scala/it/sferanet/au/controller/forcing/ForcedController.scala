package it.sferanet.au.controller.forcing

import it.sferanet.au.filterPdr.input.schema.CaForcingInputParmSchema
import it.sferanet.au.filterPdr.input.struct.CaForcingInputParmStruct
import it.sferanet.au.schema.PdrMassivoSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

object ForcedController {

  /**
   * Applica le forzature, se attive, a partire da un file csv. I campi sui quali si applicano le forzature sono:
   * `prelievo_annuo_prev_forced`,<br>
   * `cod_prof_prel_std_forced`,<br>
   * `cat_uso_forced`,<br>
   * `zona_climatica_forced`,<br>
   * `classe_prelievo_forced`,<br>
   * `trattamento_forced`
   */
  def forcing(pdrMassivo: DataFrame): DataFrame = {
    val filePath: String = Environment.getForcingCsvPath

    val csv_trattamento = "csv_trattamento"
    val CaForcingInputParmDF: DataFrame = Environment.getSqlContext.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .format("csv").schema(CaForcingInputParmStruct.struct).load(filePath)
      .withColumnRenamed(CaForcingInputParmSchema.trattamento.toString, csv_trattamento)

    pdrMassivo
      .join(broadcast(CaForcingInputParmDF), pdrMassivo(PdrMassivoSchema.codice_pdr) === CaForcingInputParmDF(CaForcingInputParmSchema.pdr.toString), "left")
      //Drop col valued with null and replace them with values from input SAG file
      .drop(PdrMassivoSchema.prelievo_annuo_prev_forced)
      .drop(PdrMassivoSchema.cod_prof_prel_std_forced)
      .drop(PdrMassivoSchema.cat_uso_forced)
      .drop(PdrMassivoSchema.zona_climatica_forced)
      .drop(PdrMassivoSchema.classe_prelievo_forced)
      .drop(PdrMassivoSchema.trattamento_forced)
      .withColumn(PdrMassivoSchema.prelievo_annuo_prev_forced, CaForcingInputParmDF.col(CaForcingInputParmSchema.ca.toString).cast(StringType))
      .drop(CaForcingInputParmDF.col(CaForcingInputParmSchema.pdr.toString))
      .drop(CaForcingInputParmDF.col(CaForcingInputParmSchema.ca.toString))
      .withColumnRenamed(CaForcingInputParmSchema.codPrel.toString, PdrMassivoSchema.cod_prof_prel_std_forced)
      .withColumnRenamed(CaForcingInputParmSchema.catUso.toString, PdrMassivoSchema.cat_uso_forced)
      .withColumnRenamed(CaForcingInputParmSchema.zonClimatica.toString, PdrMassivoSchema.zona_climatica_forced)
      .withColumnRenamed(CaForcingInputParmSchema.classePrelievo.toString, PdrMassivoSchema.classe_prelievo_forced)
      .withColumnRenamed(csv_trattamento, PdrMassivoSchema.trattamento_forced)
      //replace blank with nulls
      .withColumn(PdrMassivoSchema.prelievo_annuo_prev_forced, replaceBlankStringWithNullExpression(PdrMassivoSchema.prelievo_annuo_prev_forced))
      .withColumn(PdrMassivoSchema.cod_prof_prel_std_forced, replaceBlankStringWithNullExpression(PdrMassivoSchema.cod_prof_prel_std_forced))
      .withColumn(PdrMassivoSchema.cat_uso_forced, replaceBlankStringWithNullExpression(PdrMassivoSchema.cat_uso_forced))
      .withColumn(PdrMassivoSchema.zona_climatica_forced, replaceBlankStringWithNullExpression(PdrMassivoSchema.zona_climatica_forced))
      .withColumn(PdrMassivoSchema.classe_prelievo_forced, replaceBlankStringWithNullExpression(PdrMassivoSchema.classe_prelievo_forced))
      .withColumn(PdrMassivoSchema.trattamento_forced, replaceBlankStringWithNullExpression(PdrMassivoSchema.trattamento_forced))
  }

  private def replaceBlankStringWithNullExpression(colName: String): Column = {
    when(col(colName) =!= lit(""), col(colName))
  }
}
