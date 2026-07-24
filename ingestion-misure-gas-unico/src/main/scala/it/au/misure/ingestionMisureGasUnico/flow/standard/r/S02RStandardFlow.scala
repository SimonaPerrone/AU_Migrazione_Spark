package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.S02RSchema
import S02RSchema.annomese

object S02RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = S02RSchema

  override val hiveTableName: String = "prt_cmg_s02r_p"

  override def flowName: String = "S02R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
