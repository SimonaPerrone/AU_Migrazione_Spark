package it.sferanet.au.dal.rcugas

import it.sferanet.au.dal.Dao
import it.sferanet.au.schema.RcuAziendaPSchema
import it.sferanet.au.utilities.Environment

class RcuAziendaDao extends Dao {
  override val tableName: String = Environment.getRcuAziendaTableName
  override val columns: List[String] = List(RcuAziendaPSchema.n_id_azienda, RcuAziendaPSchema.t_piva)
}
