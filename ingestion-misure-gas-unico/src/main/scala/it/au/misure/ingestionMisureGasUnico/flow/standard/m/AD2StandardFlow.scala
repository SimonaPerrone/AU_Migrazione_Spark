package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.AD2Schema
import AD2Schema.annomese

object AD2StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = AD2Schema

  override val hiveTableName: String = "prt_cmg_ad2_p"

  override def flowName: String = "AD2"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map()
}
