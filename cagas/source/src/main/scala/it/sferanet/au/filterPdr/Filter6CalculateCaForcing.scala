package it.sferanet.au.filterPdr

import it.sferanet.au.filterPdr.input.schema.CaForcingInputParmSchema
import it.sferanet.au.filterPdr.input.struct.CaForcingInputParmStruct
import it.sferanet.au.schema.PdrMassivoSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, lit, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

@deprecated("Use forced.enable for run forced.")
class Filter6CalculateCaForcing extends FilterPdr {

  lazy val filePath: String = Environment.getForcingCsvPath

  lazy val CaForcingInputParmDF: DataFrame = Environment.getSqlContext.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .format("csv").schema(CaForcingInputParmStruct.struct).load(filePath)
    .drop("trattamento")
    .cache()

  override def getPdrs: RDD[String] = CaForcingInputParmDF.select(CaForcingInputParmSchema.pdr.toString).rdd.map(_.getString(0))

  // IMPORTANT: this filter only adds column from input file to table PdrMassivo and filter out pdrs. Then columns with
  // suffix _forced are valued properly in CaFinalController because at this point in the execution flow some required
  // data are missing
  override def filterPdrMassivo(pdrMassivo: DataFrame): DataFrame = {
    val csv_trattamento = "csv_trattamento"
    val pdrMassivoFiltered: DataFrame = super.filterPdrMassivo(pdrMassivo)

    pdrMassivoFiltered
      .join(broadcast(CaForcingInputParmDF.withColumnRenamed(CaForcingInputParmSchema.trattamento.toString, csv_trattamento)),
        pdrMassivoFiltered(PdrMassivoSchema.codice_pdr) === CaForcingInputParmDF(CaForcingInputParmSchema.pdr.toString), "inner")
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
