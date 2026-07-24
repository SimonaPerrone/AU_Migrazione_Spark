package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.CaPreFinalSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

class CaPreFinalDao extends Dao {
  override val tableName: String = Environment.getCaPreFinalTableName
  override val columns: List[String] = CaPreFinalSchema.getValues

  def readPartition(executionid: String): DataFrame = {
    readTable
      .selectExpr(columns: _*)
      .filter(col(CaPreFinalSchema.executionid) === executionid)
      .withColumn(CaPreFinalSchema.n_id_distr, col(CaPreFinalSchema.n_id_distr).cast(StringType))
      .withColumn(CaPreFinalSchema.n_id_az_udd, col(CaPreFinalSchema.n_id_az_udd).cast(StringType))
      .withColumn(CaPreFinalSchema.n_id_udb, col(CaPreFinalSchema.n_id_udb).cast(StringType))
      .repartition(2500)
  }
}



