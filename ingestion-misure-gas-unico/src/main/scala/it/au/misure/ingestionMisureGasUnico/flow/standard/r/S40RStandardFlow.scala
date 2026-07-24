package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.S40RSchema
import S40RSchema.annomese

object S40RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = S40RSchema

  override val hiveTableName: String = "prt_cmg_s40r_p"

  override def flowName: String = "S40R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
