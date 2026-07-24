package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RSLSchema
import RSLSchema.{annomese, cod_flusso, cod_servizio}

object RSLStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = RSLSchema

  override val hiveTableName: String = "prt_cmg_rsl_p"

  override def flowName: String = "RSL"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
