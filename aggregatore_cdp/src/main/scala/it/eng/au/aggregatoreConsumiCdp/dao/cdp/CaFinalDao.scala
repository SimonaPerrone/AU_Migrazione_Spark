package it.eng.au.aggregatoreConsumiCdp.dao.cdp

import it.eng.au.aggregatoreConsumiCdp.dao.Dao
import it.eng.au.aggregatoreConsumiCdp.schema.CaFinalSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StringType

class CaFinalDao extends Dao {
  override val tableName: String = Environment.getCaFinalTableName
  override val columns: List[String] = CaFinalSchema.getValues

  def readPartition(executionid: String): DataFrame = {
    readTable
      .filter(col(CaFinalSchema.executionid) === executionid)
      .withColumn(CaFinalSchema.n_id_distr, col(CaFinalSchema.n_id_distr).cast(StringType))
      .withColumn(CaFinalSchema.n_id_az_udd, col(CaFinalSchema.n_id_az_udd).cast(StringType))
      .withColumn(CaFinalSchema.n_id_udb, col(CaFinalSchema.n_id_udb).cast(StringType))
      .withColumn(CaFinalSchema.pres_tds, when(col(CaFinalSchema.pres_tds), lit("SI")).otherwise(lit("NO")))
      .selectExpr(columns: _*)
      .repartition(2500)
  }
}
