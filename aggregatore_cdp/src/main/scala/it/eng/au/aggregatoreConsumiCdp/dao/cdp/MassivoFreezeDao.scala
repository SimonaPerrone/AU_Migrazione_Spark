package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.MassivoFreezeSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class MassivoFreezeDao extends Dao {
  override val tableName: String = Environment.getRcugasMassivoFreezeTableName
  override val columns: List[String] = MassivoFreezeSchema.getValues

  def readPartition(executionid: Long): DataFrame = {
    readTable
      .selectExpr(columns: _*)
      .filter(col(MassivoFreezeSchema.execution_id) === executionid)
  }
}
