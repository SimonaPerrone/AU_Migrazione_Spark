package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.V01RSchema
import V01RSchema.annomese

object V01RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = V01RSchema

  override val hiveTableName: String = "prt_cmg_v01r_p"

  override def flowName: String = "V01R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
