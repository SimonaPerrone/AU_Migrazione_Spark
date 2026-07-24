package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.AD5Schema
import AD5Schema.annomese

object AD5StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = AD5Schema

  override val hiveTableName: String = "prt_cmg_ad5_p"

  override def flowName: String = "AD5"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
