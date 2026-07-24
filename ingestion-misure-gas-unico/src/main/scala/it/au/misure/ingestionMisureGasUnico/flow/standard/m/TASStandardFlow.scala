package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.TASSchema
import TASSchema.{annomese, cod_flusso, cod_servizio}

object TASStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = TASSchema

  override val hiveTableName: String = "prt_cmg_tas_p"

  override def flowName: String = "TAS"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
