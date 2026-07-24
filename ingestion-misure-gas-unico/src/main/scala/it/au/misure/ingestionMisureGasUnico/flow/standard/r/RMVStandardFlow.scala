package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RMVSchema
import RMVSchema.{annomese, cod_flusso, cod_servizio}

object RMVStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = RMVSchema

  override val hiveTableName: String = "prt_cmg_rmv_p"

  override def flowName: String = "RMV"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )
}
