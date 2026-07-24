package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.FDDSchema
import FDDSchema.{annomese, cod_flusso, cod_servizio}

object FDDStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = FDDSchema

  override val hiveTableName: String = "prt_cmg_def_p"

  override def flowName: String = "FDD"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
