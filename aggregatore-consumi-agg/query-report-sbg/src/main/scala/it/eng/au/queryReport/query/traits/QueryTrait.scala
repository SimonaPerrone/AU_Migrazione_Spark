package it.eng.au.queryReport.query.traits

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.schema.ValidatedFlowsSchema
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp

/**
 * Contiene i valori e i metodi base utilizzati dalle varie query. Lo scopo di un oggetto query è eseguire la relativa aggregazione
 * e scrivere il risultato su una tabella di reportistica
 */
trait QueryTrait extends Serializable {
  /** Nome della query da lanciare. */
  val queryName: String
  /** Scheda di output della tabella di reportistica. */
  val outputSchema: SchemaEnum
  /** Percorso di output della tabella di reportistica */
  def hdfsOutputPath: String
  /** Nome della tabella di reportistica */
  def tableName: String

  /** Esegue la query e scrive il risultato su tabella. */
  def runQuery(df: DataFrame): Unit = {
    val aggregatoDf = getQueryDF(df)
    writeOnHive(aggregatoDf)
  }

  /**
   * Esegue la query richiamando la funzione [[getAggregato]] dalla rispettiva pubblicazione in SBG.
   * Dopodiché aggiunge delle colonne per predisporre il dataframe alla scrittura.
   * @param df dataframe dei consumi
   * @return dataframe aggregata secondo la query considerata
   */
  def getQueryDF(df: DataFrame): DataFrame = {
    getAggregato(df)
      .withColumn("dailyconsumption_executionid", lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn("executionid", lit(Timestamp.valueOf(Environment.getDateRun).getTime))
      .selectExpr(outputSchema.getValues: _*)
  }

  def writeOnHive(df: DataFrame): Unit = {
    df.write
      .partitionBy("executionid").mode(SaveMode.Append).parquet(hdfsOutputPath)

    if (!Environment.isLocalMode) Environment.spark.sql(s"MSCK REPAIR TABLE $tableName")
  }

  def getAggregato(df: DataFrame): DataFrame

  /**
   * Legge la tabella dei flussi validati, utilizzata per estrarre l'elenco flussi.
   * @return dataframe dei flussi validati
   */
  def getAndPrepareValidateFlow(): DataFrame = {
    Environment.sqlContext.table(Environment.getValidatedFlowTableName)
      .selectExpr(ValidatedFlowsSchema.getValues: _*)
      .filter(col(ValidatedFlowsSchema.executionid) === Environment.getDailyConsumptionExecutionid)
      .drop(col(ValidatedFlowsSchema.executionid))
  }
}
