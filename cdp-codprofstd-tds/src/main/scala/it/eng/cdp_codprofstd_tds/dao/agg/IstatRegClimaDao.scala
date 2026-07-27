package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.IstatRegClimaSchema
import it.eng.cdp_codprofstd_tds.utility.Environment

class IstatRegClimaDao extends Dao {
  override val tableName: String = Environment.getPrtIstatRegClimaTableName
  override val columns: List[String] = IstatRegClimaSchema.getValues
}
