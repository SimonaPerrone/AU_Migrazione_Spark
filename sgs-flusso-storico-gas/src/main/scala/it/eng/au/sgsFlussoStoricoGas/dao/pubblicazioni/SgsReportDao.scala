package it.eng.au.sgsFlussoStoricoGas.dao.pubblicazioni

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.pubblicazione.SgsReportSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

class SgsReportDao extends Dao {
  override val tablePath: String = Environment.getSgsReportPath
  override val columns: List[String] = SgsReportSchema.getValues
  val tableName: String = Environment.getSgsReportTableName

  def writeParquet(df: DataFrame): Unit = {
    df
      .coalesce(5)
      .write
      .mode(SaveMode.Append)
      .parquet(tablePath)

    Environment.getSpark.sql(s"REFRESH $tableName")
  }
}
