package it.sferanet.au.dal.rcugas

import it.sferanet.au.dal.Dao
import it.sferanet.au.schema.RcugasDistributoreSchema
import it.sferanet.au.utilities.Environment

class RcugasDistributoreDao extends Dao {
  override val tableName: String = Environment.getRcugasDistributoreTableName
  override val columns: List[String] = RcugasDistributoreSchema.getValues
}
