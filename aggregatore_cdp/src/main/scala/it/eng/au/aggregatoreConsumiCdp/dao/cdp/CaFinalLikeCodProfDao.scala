package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.CaFinalLikeSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class CaFinalLikeCodProfDao extends Dao {
  override val tableName: String = Environment.getCodProfTableName
  override val columns: List[String] = CaFinalLikeSchema.getValues

  def readPartition(executionid: String): DataFrame = {
    readTable
      .filter(col(CaFinalLikeSchema.execution_id) === executionid)
  }
}
