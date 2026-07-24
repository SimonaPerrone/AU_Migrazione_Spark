package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.V02RSchema
import V02RSchema.annomese

object V02RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = V02RSchema

  override val hiveTableName: String = "prt_cmg_v02r_p"

  override def flowName: String = "V02R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
