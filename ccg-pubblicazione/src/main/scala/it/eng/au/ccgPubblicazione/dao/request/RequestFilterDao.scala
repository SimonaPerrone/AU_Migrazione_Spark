package it.eng.au.ccgPubblicazione.dao.request

import it.eng.au.ccgPubblicazione.schema.request.RequestFilterSchema
import it.eng.au.ccgPubblicazione.utility.Environment

/** Tabella delle richieste di tipo filtro. */
object RequestFilterDao extends RequestDao {
  override val tableName: String = Environment.getRequestFilterTableName
  override val fields: List[String] = RequestFilterSchema.getValues
  override val servizioField: String = RequestFilterSchema.T_PROCESSO
  override val idRequestFiled: String = RequestFilterSchema.N_ID_RICHIESTA
  override val partitionField: String = RequestFilterSchema.PARTITION_REQUEST_DATE
}
