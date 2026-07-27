package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.PrtVsgSchema
import it.eng.cdp_codprofstd_tds.utility.Environment

class PrtVsgDao extends Dao {
  override val tableName: String = Environment.getPrtVsgTableName
  override val columns: List[String] = PrtVsgSchema.getValues
}
