package it.eng.au.ccgPubblicazione.dao.request

import it.eng.au.ccgPubblicazione.schema.request.RequestEsitoSchema
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

/** Tabella di esito del processo di pubblicazione CCG. */
object RequestEsitoDao {

  val tableName: String = Environment.getRequestEsitoTableName
  val tableNameExport: String = Environment.getRequestEsitoTableNameExport

  /** Scrive nella tabella storicizzata. */
  def writePartition(df: DataFrame): Unit = {
    df
      .coalesce(10)
      .write
      .partitionBy(RequestEsitoSchema.D_DATA_RICHIESTA, RequestEsitoSchema.sessione, RequestEsitoSchema.executionid)
      .mode(SaveMode.Append)
      .parquet(Environment.getRequestEsitoParquet)

    if(!Environment.isLocalMode) Environment.spark.sql(s"msck repair table $tableName")
  }

  /** Scrive nella tabella adibita allo sqoop export. */
  def write(df: DataFrame): Unit = {
    df
      .coalesce(10)
      .write.mode(SaveMode.Append).parquet(Environment.getRequestEsitoParquetExport)

  }
}
