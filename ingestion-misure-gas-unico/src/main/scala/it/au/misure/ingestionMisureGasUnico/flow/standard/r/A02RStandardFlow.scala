package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.A02RSchema
import A02RSchema.annomese

object A02RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = A02RSchema

  override val hiveTableName: String = "prt_cmg_a02r_p"

  override def flowName: String = "A02R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
