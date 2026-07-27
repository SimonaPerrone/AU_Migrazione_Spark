package it.eng.au.ccgPubblicazione.dao.rcugas

import it.eng.au.ccgPubblicazione.dao.Dao
import it.eng.au.ccgPubblicazione.schema.rcugas.RcugasVarMisuratoreSchema
import it.eng.au.ccgPubblicazione.utility.Environment

class RcugasVarMisuratoreDao extends Dao {
  override val tableName: String = Environment.getRcugasVarMisuratoreTableName
  override val fields: List[String] = RcugasVarMisuratoreSchema.getValues
}
