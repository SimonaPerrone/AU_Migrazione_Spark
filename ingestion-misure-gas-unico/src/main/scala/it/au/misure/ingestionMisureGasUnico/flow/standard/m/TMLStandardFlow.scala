package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.TMLSchema
import TMLSchema.{annomese, cod_flusso, cod_servizio}

object TMLStandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = TMLSchema

  override val hiveTableName: String = "prt_cmg_tml_p"

  override def flowName: String = "TML"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
