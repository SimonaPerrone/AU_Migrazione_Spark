package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.flow.standard.m.MisuraFlow
import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.AD4RSchema
import AD4RSchema.annomese

object AD4RStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = AD4RSchema

  override val hiveTableName: String = "prt_cmg_ad4r_p"

  override def flowName: String = "AD4R"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
