package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.A02Schema
import A02Schema.{annomese, cod_flusso, cod_servizio}

object A02StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = A02Schema

  override val hiveTableName: String = "prt_cmg_a02_p"

  override def flowName: String = "A02"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
