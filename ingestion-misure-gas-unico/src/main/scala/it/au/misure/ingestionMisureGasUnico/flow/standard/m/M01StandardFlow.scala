package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.M01Schema
import M01Schema.{annomese, cod_flusso, cod_servizio}

object M01StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = M01Schema

  override val hiveTableName: String = "prt_cmg_m01_p"

  override def flowName: String = "M01"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
