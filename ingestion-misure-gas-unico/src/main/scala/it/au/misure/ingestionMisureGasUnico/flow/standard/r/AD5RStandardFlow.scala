package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.flow.standard.m.MisuraFlow
import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.AD5RSchema
import AD5RSchema.annomese

object AD5RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = AD5RSchema

  override val hiveTableName: String = "prt_cmg_ad5r_p"

  override def flowName: String = "AD5R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
