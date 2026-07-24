package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.D01RSchema
import D01RSchema.annomese

object D01RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = D01RSchema

  override val hiveTableName: String = "prt_cmg_d01r_p"

  override def flowName: String = "D01R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
