package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.D02Schema
import D02Schema.annomese

object D02StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = D02Schema

  override val hiveTableName: String = "prt_cmg_d02_p"

  override def flowName: String = "D02"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
