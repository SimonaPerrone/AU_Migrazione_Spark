package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.RcugasUdbSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment

class RcugasUdbDao extends Dao {
  override val tableName: String = Environment.getRcugasUdbTableName
  override val columns: List[String] = RcugasUdbSchema.getValues
}
