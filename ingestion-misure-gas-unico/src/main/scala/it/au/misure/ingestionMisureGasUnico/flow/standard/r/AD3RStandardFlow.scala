package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.AD3RSchema
import AD3RSchema.annomese

object AD3RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = AD3RSchema

  override val hiveTableName: String = "prt_cmg_ad3r_p"

  override def flowName: String = "AD3R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
