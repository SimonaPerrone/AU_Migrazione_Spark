package it.eng.au.cceCalcolo.dao.calcoloOutput

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.calcoloOutput.CceCalcoloPRSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class CceCalcoloPRDAO extends Dao {
  override val tablePath: String = Properties.getCceCalcoloPRTablePath
  override val tableName: String = Properties.getCceCalcoloPRTableName
  override val columns: List[String] = CceCalcoloPRSchema.getValues
  val executionId = "executionId"

  def writeOnHive(df: DataFrame): Unit = {
    df
      .withColumn(executionId, lit(Environment.executionId.toString))
      .write
      .mode(SaveMode.Append)
      .partitionBy(CceCalcoloPRSchema.anno, CceCalcoloPRSchema.mese, executionId)
      .format("parquet")
      .save(tablePath)

    Environment.spark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
