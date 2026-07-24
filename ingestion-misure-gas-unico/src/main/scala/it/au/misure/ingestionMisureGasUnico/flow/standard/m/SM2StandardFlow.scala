package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.SM2Schema
import SM2Schema.{annomese, cod_flusso, cod_servizio}

object SM2StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = SM2Schema

  override val hiveTableName: String = "prt_cmg_sm2_p"

  override def flowName: String = "SM2"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
