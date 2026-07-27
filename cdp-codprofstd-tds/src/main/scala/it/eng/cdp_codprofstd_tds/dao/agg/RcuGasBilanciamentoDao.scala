package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.RcuGasBilanciamentoSchema
import org.apache.spark.sql.{DataFrame, SQLContext}

import it.eng.cdp_codprofstd_tds.utility.Environment

class RcuGasBilanciamentoDao extends Dao {
  override val tableName: String = Environment.getRcugasBilanciamentoTableName
  override val columns: List[String] = RcuGasBilanciamentoSchema.getValues
}
