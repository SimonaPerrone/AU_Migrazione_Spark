package it.eng.au.aggiustamentoGas.dao.agg

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

trait AggDao extends Dao {
  val EXECUTION_ID = "executionid"
  val SESSION = "session"
  val tableName: String

  def writeParquet(df: DataFrame): Unit = {
    df
      .withColumn(SESSION, lit(Environment.getSession))
      .withColumn(EXECUTION_ID, lit(Environment.executionId))
      .write
      .mode(SaveMode.Append)
      .partitionBy(SESSION, EXECUTION_ID)
      .parquet(parquetPath)

    Environment.getSpark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
