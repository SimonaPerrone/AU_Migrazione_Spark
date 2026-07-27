package it.eng.au.ccgPubblicazione.dao.rcugas

import it.eng.au.ccgPubblicazione.dao.Dao
import it.eng.au.ccgPubblicazione.schema.rcugas.RcugasPdrSchema
import it.eng.au.ccgPubblicazione.utility.Environment

class RcugasPdrDao extends Dao {
  override val tableName: String = Environment.getRcugasPdrTableName
  override val fields: List[String] = RcugasPdrSchema.getValues
}
