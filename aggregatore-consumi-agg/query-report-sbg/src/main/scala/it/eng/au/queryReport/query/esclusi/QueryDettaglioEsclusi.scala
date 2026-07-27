package it.eng.au.queryReport.query.esclusi

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum
import it.eng.au.queryReport.query.traits.QueryTrait
import org.apache.spark.sql.DataFrame

object QueryDettaglioEsclusi extends QueryTrait {
  override val queryName: String = "esclusi"
  override val tableName: String = ""

  override def runQuery(df: DataFrame): Unit = {
    val validateFlow = getAndPrepareValidateFlow()

    val elencoFlussiDettaglioEsclusi = QueryElencoFlussiDettaglioEsclusi.getQueryDF(df, validateFlow)
    QueryElencoFlussiDettaglioEsclusi.writeOnHive(elencoFlussiDettaglioEsclusi)

    val pdrDettaglioIncoerenti = QueryPdrDettaglioEsclusi.getQueryDF(df)
    QueryPdrDettaglioEsclusi.writeOnHive(pdrDettaglioIncoerenti)
  }

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df
  override val outputSchema: SchemaEnum = new SchemaEnum {}
  override def hdfsOutputPath: String = ""
}