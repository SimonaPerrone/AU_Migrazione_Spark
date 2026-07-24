package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.AD3Schema
import AD3Schema.annomese

object AD3StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = AD3Schema

  override val hiveTableName: String = "prt_cmg_ad3_p"

  override def flowName: String = "AD3"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
