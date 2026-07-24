package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.D02RSchema
import D02RSchema.annomese

object D02RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = D02RSchema

  override val hiveTableName: String = "prt_cmg_d02r_p"

  override def flowName: String = "D02R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
