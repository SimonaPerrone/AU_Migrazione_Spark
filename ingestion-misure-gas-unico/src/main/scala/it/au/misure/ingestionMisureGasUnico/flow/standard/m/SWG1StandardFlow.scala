package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.SWG1Schema
import SWG1Schema.{annomese, cod_flusso, cod_servizio}

object SWG1StandardFlow extends MisuraFlow {
  override val schema: SchemaEnum = SWG1Schema

  override val hiveTableName: String = "prt_cmg_sw1_p"

  override def flowName: String = "SWG1"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
