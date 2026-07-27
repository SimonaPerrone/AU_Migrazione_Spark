package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.RcuGasConnessioniDistr2Schema
import it.eng.cdp_codprofstd_tds.utility.Environment

class RcuGasConnessioniDistr2Dao extends Dao {
  override val tableName: String = Environment.getRcugasConnessioniDistr2TableName
  override val columns: List[String] = RcuGasConnessioniDistr2Schema.getValues
}
