package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.A40Schema
import A40Schema.{annomese, cod_flusso, cod_servizio}

object A40StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = A40Schema

  override val hiveTableName: String = "prt_cmg_a40_0150_p"

  override def flowName: String = "A40"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
