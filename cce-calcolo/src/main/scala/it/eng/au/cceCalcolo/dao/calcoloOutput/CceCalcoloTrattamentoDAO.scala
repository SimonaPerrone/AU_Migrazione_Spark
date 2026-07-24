package it.eng.au.cceCalcolo.dao.calcoloOutput

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.calcoloOutput.CceCalcoloTrattamentoSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class CceCalcoloTrattamentoDAO extends Dao {
  override val tablePath: String = Properties.getCceCalcoloTrattamentoTablePath
  override val tableName: String = Properties.getCceCalcoloTrattamentoTableName
  override val columns: List[String] = CceCalcoloTrattamentoSchema.getValues
  val executionId = "executionId"

  def writeOnHive(df: DataFrame): Unit = {
    df
      .withColumn(executionId, lit(Environment.executionId.toString))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(CceCalcoloTrattamentoSchema.t_anno_mese)
      .format("parquet")
      .save(tablePath)

    Environment.spark.sql(s"MSCK REPAIR TABLE $tableName")
  }

}
