package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.RcuGasMassivoSchema
import it.eng.cdp_codprofstd_tds.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class RcuGasMassivoDao extends Dao {
  override val tableName: String = Environment.getRcugasMassivoTableName
  override val columns: List[String] = RcuGasMassivoSchema.getValues

  def readPartition(executionId: String): DataFrame = {
    readTable.filter(col(RcuGasMassivoSchema.execution_id) === executionId)
  }
}
