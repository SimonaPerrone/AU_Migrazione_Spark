package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.A01RSchema
import A01RSchema.annomese

object A01RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = A01RSchema

  override val hiveTableName: String = "prt_cmg_a01r_p"

  override def flowName: String = "A01R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
