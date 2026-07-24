package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.V02Schema
import V02Schema.{annomese, cod_flusso, cod_servizio}

object V02StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = V02Schema

  override val hiveTableName: String = "prt_cmg_v02_p"

  override def flowName: String = "V02"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
