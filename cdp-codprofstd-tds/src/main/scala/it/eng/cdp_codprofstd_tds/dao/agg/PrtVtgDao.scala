package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.PrtVtgSchema
import it.eng.cdp_codprofstd_tds.utility.Environment

class PrtVtgDao extends Dao {
  override val tableName: String = Environment.getPrtVtgTableName
  override val columns: List[String] = PrtVtgSchema.getValues
}
