package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.TGLSchema
import TGLSchema.{cod_flusso, cod_servizio, mese_comp}
import it.au.misure.ingestionMisureGasUnico.model.schema.CommonColumnsSchema._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.regexp_replace

object TGLStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = TGLSchema

  override val hiveTableName: String = "prt_cmg_tgl_p"

  override def flowName: String = "TGL"

  override val partitioningColumns: List[String] = List(
    mese_comp.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )

  override def addCommonColumns(df: DataFrame, unzipTimestamp: String): DataFrame = {
    val commonDf = super.addCommonColumns(df, unzipTimestamp)
      .withColumn(mese_comp, regexp_replace(df(mese_comp), "/", ""))

    commonDf
      .na.fill(EE.toString, List(mese_comp.toString))
  }
}
