package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.A01Schema
import A01Schema.{annomese, cod_flusso, cod_servizio}

object A01StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = A01Schema

  override val hiveTableName: String = "prt_cmg_a01_0150_p"

  override def flowName: String = "A01"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
