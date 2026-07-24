package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.TAVSchema
import TAVSchema.{annomese, cod_flusso, cod_servizio}

object TAVStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = TAVSchema

  override val hiveTableName: String = "prt_cmg_tav_p"

  override def flowName: String = "TAV"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
