package it.eng.au.cceCalcolo.dao.calcoloOutput

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.calcoloOutput.CceCalcoloPReinSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class CceCalcoloPReinDAO extends Dao {
  override val tablePath: String = Properties.getCceCalcoloPReinTablePath
  override val tableName: String = Properties.getCceCalcoloPReinTableName
  override val columns: List[String] = CceCalcoloPReinSchema.getValues
  val executionId = "executionId"

  def writeOnHive(df: DataFrame): Unit = {
    df
      .withColumn(executionId, lit(Environment.executionId.toString))
      .write
      .mode(SaveMode.Append)
      .partitionBy(CceCalcoloPReinSchema.anno, CceCalcoloPReinSchema.mese, executionId)
      .format("parquet")
      .save(tablePath)

    Environment.spark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
