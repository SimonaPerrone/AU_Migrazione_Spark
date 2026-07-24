package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.TMVSchema
import TMVSchema.{annomese, cod_flusso, cod_servizio}

object TMVStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = TMVSchema

  override val hiveTableName: String = "prt_cmg_tmv_p"

  override def flowName: String = "TMV"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
