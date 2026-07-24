package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.TALSchema
import TALSchema.{annomese, cod_flusso, cod_servizio}

object TALStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = TALSchema

  override val hiveTableName: String = "prt_cmg_tal_p"

  override def flowName: String = "TAL"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
