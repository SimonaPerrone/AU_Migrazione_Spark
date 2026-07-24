package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.RcugasDistributoreSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment

class RcugasDistributoreDao extends Dao {
  override val tableName: String = Environment.getRcugasDistributoreTableName
  override val columns: List[String] = RcugasDistributoreSchema.getValues
}
