package it.eng.au.ccgPubblicazione.dao.cdp

import it.eng.au.ccgPubblicazione.dao.PartitionedDao
import it.eng.au.ccgPubblicazione.schema.cdp.{CaFinalCdpSchema, ValidatedFlowsCdpSchema}
import it.eng.au.ccgPubblicazione.utility.Environment

/** Tabella dei flussi validati per il processo CDP FIN/RIC. */
object ValidatedFlowsCdpDao extends PartitionedDao {
  override val tableName: String = Environment.getCdpValidatedFlowTableName
  override val partitionField: String = ValidatedFlowsCdpSchema.executionid.toString
  override val fields: List[String] = ValidatedFlowsCdpSchema.getValues

}
