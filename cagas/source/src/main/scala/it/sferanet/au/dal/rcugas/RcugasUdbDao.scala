package it.sferanet.au.dal.rcugas

import it.sferanet.au.dal.Dao
import it.sferanet.au.schema.RcuGasUdbPSchema
import it.sferanet.au.utilities.Environment

class RcugasUdbDao extends Dao {
  override val tableName: String = Environment.getRcugasUdbTableName
  override val columns: List[String] = List(RcuGasUdbPSchema.n_id_udb, RcuGasUdbPSchema.n_id_azienda)
}
