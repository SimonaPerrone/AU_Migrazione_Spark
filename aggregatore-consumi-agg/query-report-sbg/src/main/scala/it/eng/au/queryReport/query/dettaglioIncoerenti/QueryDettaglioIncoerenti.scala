package it.eng.au.queryReport.query.dettaglioIncoerenti

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.ValidatedFlowsSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** Esegue la query del dettaglio incoerenti (incoerenti GDM), comprensiva di dataframe dei consumi e di elenco flussi. */
object QueryDettaglioIncoerenti extends QueryTrait {
  override val queryName: String = "dettaglioIncoerenti"
  override val tableName: String = ""

  override def runQuery(df: DataFrame): Unit = {
    val validateFlow = getAndPrepareValidateFlow()

    val elencoFlussiDettaglioIncoerenti = QueryElencoFlussiDettaglioIncoerenti.getQueryDF(df, validateFlow)
    QueryElencoFlussiDettaglioIncoerenti.writeOnHive(elencoFlussiDettaglioIncoerenti)

    val pdrDettaglioIncoerenti = QueryPdrDettaglioIncoerenti.getQueryDF(df)
    QueryPdrDettaglioIncoerenti.writeOnHive(pdrDettaglioIncoerenti)
  }

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df
  override val outputSchema: SchemaEnum = new SchemaEnum {}
  override def hdfsOutputPath: String = ""
}