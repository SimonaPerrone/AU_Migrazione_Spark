package it.eng.au.sgsFlussoStoricoGas.dao.pubblicazioni

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.pubblicazione.PubblicazioneInfoSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit

class PubblicazioneInfoDao extends Dao {

  override val tablePath: String = Environment.getPubblicazioneInfoPath
  override val columns: List[String] = PubblicazioneInfoSchema.getValues
  val tableName: String = Environment.getPubblicazioneInfoTableName
  val executionId = "execution_id"

  def writeParquet(df: DataFrame): Unit = {
    df
      .coalesce(5)
      .withColumn(executionId, lit(Environment.executionId))
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Append)
      .partitionBy(executionId)
      .parquet(tablePath)

    Environment.getSpark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
