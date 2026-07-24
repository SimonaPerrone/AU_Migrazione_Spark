package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.R40Schema
import R40Schema.annomese

object R40StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = R40Schema

  override val hiveTableName: String = "prt_cmg_r40_p"

  override def flowName: String = "R40"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
