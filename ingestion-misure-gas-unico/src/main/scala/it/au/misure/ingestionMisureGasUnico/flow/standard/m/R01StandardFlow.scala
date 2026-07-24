package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.R01Schema
import R01Schema.{annomese, cod_flusso, cod_servizio}

object R01StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = R01Schema

  override val hiveTableName: String = "prt_cmg_r01_p"

  override def flowName: String = "R01"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
