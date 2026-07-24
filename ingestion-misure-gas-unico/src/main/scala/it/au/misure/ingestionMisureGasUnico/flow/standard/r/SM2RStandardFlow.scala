package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.SM2RSchema
import SM2RSchema.annomese

object SM2RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = SM2RSchema

  override val hiveTableName: String = "prt_cmg_sm2r_p"

  override def flowName: String = "SM2R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
