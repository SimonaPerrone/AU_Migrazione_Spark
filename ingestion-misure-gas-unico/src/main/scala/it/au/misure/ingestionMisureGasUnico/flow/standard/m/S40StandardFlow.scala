package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.S40Schema
import S40Schema.annomese

object S40StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = S40Schema

  override val hiveTableName: String = "prt_cmg_s40_p"

  override def flowName: String = "S40"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
