package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.SM1RSchema
import SM1RSchema.annomese

object SM1RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = SM1RSchema

  override val hiveTableName: String = "prt_cmg_sm1r_p"

  override def flowName: String = "SM1R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
