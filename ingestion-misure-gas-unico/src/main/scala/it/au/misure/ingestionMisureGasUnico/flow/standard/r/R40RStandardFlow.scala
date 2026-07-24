package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.R40RSchema
import R40RSchema.annomese

object R40RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = R40RSchema

  override val hiveTableName: String = "prt_cmg_r40r_p"

  override def flowName: String = "R40R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
