package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.M01RSchema
import M01RSchema.annomese

object M01RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = M01RSchema

  override val hiveTableName: String = "prt_cmg_m01r_p"

  override def flowName: String = "M01R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
