package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.AD4Schema
import AD4Schema.annomese

object AD4StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = AD4Schema

  override val hiveTableName: String = "prt_cmg_ad4_p"

  override def flowName: String = "AD4"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
