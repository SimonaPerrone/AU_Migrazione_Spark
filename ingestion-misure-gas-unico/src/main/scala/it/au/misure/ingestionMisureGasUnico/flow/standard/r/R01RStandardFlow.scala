package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.R01RSchema
import R01RSchema.annomese

object R01RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = R01RSchema

  override val hiveTableName: String = "prt_cmg_r01r_p"

  override def flowName: String = "R01R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
