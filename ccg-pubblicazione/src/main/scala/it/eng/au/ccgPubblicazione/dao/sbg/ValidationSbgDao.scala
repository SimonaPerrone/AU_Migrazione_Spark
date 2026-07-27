package it.eng.au.ccgPubblicazione.dao.sbg

import it.eng.au.ccgPubblicazione.dao.PartitionedDao
import it.eng.au.ccgPubblicazione.schema.aggsbg.ValidatedFlowsAggSchema
import it.eng.au.ccgPubblicazione.utility.Environment

/** Tabella dei flussi validati di AGG. */
object ValidationSbgDao extends PartitionedDao {
  override val tableName: String = Environment.getSbgValidatedFlowTableName
  override val partitionField: String = ValidatedFlowsAggSchema.executionid.toString
  override val fields: List[String] = ValidatedFlowsAggSchema.getValues
}
