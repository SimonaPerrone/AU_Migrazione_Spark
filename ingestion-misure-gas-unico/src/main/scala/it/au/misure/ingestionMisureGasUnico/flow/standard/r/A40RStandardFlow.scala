package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.A40RSchema
import A40RSchema.annomese

object A40RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = A40RSchema

  override val hiveTableName: String = "prt_cmg_a40r_p"

  override def flowName: String = "A40R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
