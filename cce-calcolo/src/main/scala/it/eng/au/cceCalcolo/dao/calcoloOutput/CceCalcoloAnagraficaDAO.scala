package it.eng.au.cceCalcolo.dao.calcoloOutput

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.calcoloOutput.CceCalcoloAnagraficaSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class CceCalcoloAnagraficaDAO extends Dao {
  override val tablePath: String = Properties.getCceCalcoloAnagraficaTablePath
  override val tableName: String = Properties.getCceCalcoloAnagraficaTableName
  override val columns: List[String] = CceCalcoloAnagraficaSchema.getValues
  val executionId = "executionId"

  def writeOnHive(df: DataFrame): Unit = {
    df
      .withColumn(executionId, lit(Environment.executionId.toString))
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(tablePath)

    Environment.spark.sql(s"REFRESH $tableName")
  }
}
