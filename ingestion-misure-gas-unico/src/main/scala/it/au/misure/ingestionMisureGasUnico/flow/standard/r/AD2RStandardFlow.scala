package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.AD2RSchema
import AD2RSchema.annomese

object AD2RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = AD2RSchema

  override val hiveTableName: String = "prt_cmg_ad2r_p"

  override def flowName: String = "AD2R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
