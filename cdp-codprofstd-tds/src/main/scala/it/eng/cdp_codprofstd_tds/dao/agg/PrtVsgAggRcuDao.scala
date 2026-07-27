package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.PrtVsgAggRcuSchema
import it.eng.cdp_codprofstd_tds.utility.Environment

class PrtVsgAggRcuDao extends Dao {
  override val tableName: String = Environment.getPrtVsgAggRcuTableName
  override val columns: List[String] = PrtVsgAggRcuSchema.getValues
}
