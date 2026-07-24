package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.D01Schema
import D01Schema.{annomese, cod_flusso, cod_servizio}

object D01StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = D01Schema

  override val hiveTableName: String = "prt_cmg_d01_0150_p"

  override def flowName: String = "D01"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
