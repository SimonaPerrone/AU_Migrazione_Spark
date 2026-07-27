package it.eng.au.sgsFlussoStoricoGas.dao.pubblicazioni

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.pubblicazione.PubblicazioneInfoDettSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

class PubblicazioneInfoDettDao extends Dao {

  override val tablePath: String = Environment.getPubblicazioneInfoDettPath
  override val columns: List[String] = PubblicazioneInfoDettSchema.getValues
  val tableName: String = Environment.getPubblicazioneInfoDettTableName

  def writeParquet(df: DataFrame): Unit = {
    df
      .coalesce(5)
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Append)
      .parquet(tablePath)

    Environment.getSpark.sql(s"REFRESH $tableName")
  }
}
