package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.CaSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class CaDao extends Dao {
  override val tableName: String = Environment.getCaTableName
  override val columns: List[String] = CaSchema.getValues

  def readPartition(executionId: String): DataFrame = {
    readTable
      .selectExpr(columns: _*)
      .filter(col(CaSchema.executionid) === executionId)
  }
}
