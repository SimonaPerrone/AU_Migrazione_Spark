package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.SM1Schema
import SM1Schema.{annomese, cod_flusso, cod_servizio}

object SM1StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = SM1Schema

  override val hiveTableName: String = "prt_cmg_sm1_0150_p"

  override def flowName: String = "SM1"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
