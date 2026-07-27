package it.eng.au.ccgPubblicazione.dao.request

import it.eng.au.ccgPubblicazione.schema.request._
import it.eng.au.ccgPubblicazione.utility.Environment

/** Tabella delle richieste di tipo PdR. */
object RequestPdrDao extends RequestDao {
  override val tableName: String = Environment.getRequestPdrTableName
  override val fields: List[String] = RequestPdrSchema.getValues
  override val servizioField: String = RequestPdrSchema.T_PROCESSO
  override val idRequestFiled: String = RequestPdrSchema.N_ID_RICHIESTA
  override val partitionField: String = RequestPdrSchema.PARTITION_REQUEST_DATE
}
