package it.eng.au.queryReport.query.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.ValidatedFlowsSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** Esegue la query del dettaglio unico, comprensiva di dataframe dei consumi e di elenco flussi. */
object QueryDettaglioUnico extends QueryTrait {
  override val queryName: String = "dettaglioUnico"
  override val tableName: String = ""

  override def runQuery(df: DataFrame): Unit = {
    val validateFlow = getAndPrepareValidateFlow()

    val elencoFlussiDettaglioUnico = QueryElencoFlussiDettaglioUnico.getQueryDF(df, validateFlow)
    QueryElencoFlussiDettaglioUnico.writeOnHive(elencoFlussiDettaglioUnico)

    val pdrDettaglioUnico = QueryPdrDettaglioUnico.getQueryDF(df)
    QueryPdrDettaglioUnico.writeOnHive(pdrDettaglioUnico)
  }

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df

  override val outputSchema: SchemaEnum = new SchemaEnum {}
  override def hdfsOutputPath: String = ""
}