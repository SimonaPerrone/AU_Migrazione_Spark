package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.S02Schema
import S02Schema.annomese

object S02StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = S02Schema

  override val hiveTableName: String = "prt_cmg_s02_p"

  override def flowName: String = "S02"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
