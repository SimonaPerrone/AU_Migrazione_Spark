package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.PrtVtgAggRcuSchema
import org.apache.spark.sql.{DataFrame, SQLContext}

import it.eng.cdp_codprofstd_tds.utility.Environment

class PrtVtgAggRcuDao extends Dao {
  override val tableName: String = Environment.getPrtVtgAggRcuTableName
  override val columns: List[String] = PrtVtgAggRcuSchema.getValues
}
