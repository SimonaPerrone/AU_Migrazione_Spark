package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.FUISchema
import FUISchema.{annomese, cod_flusso, cod_servizio}

object FUIStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = FUISchema

  override val hiveTableName: String = "prt_cmg_fui_p"

  override def flowName: String = "FUI"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
