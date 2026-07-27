package it.eng.au.queryReport.query.giroContatore

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum
import it.eng.au.queryReport.query.traits.QueryTrait
import org.apache.spark.sql.DataFrame

object QueryDettaglioGiroContatore extends QueryTrait {
  override val queryName: String = "dettaglioGiroContatore"
  override val tableName: String = ""

  override def runQuery(df: DataFrame): Unit = {
    val validateFlow = getAndPrepareValidateFlow()

    val elencoFlussiDettaglioGiroContatore = QueryElencoFlussiDettaglioGiroContatore.getQueryDF(df, validateFlow)
    QueryElencoFlussiDettaglioGiroContatore.writeOnHive(elencoFlussiDettaglioGiroContatore)

    val pdrDettaglioGiroContatore = QueryPdrDettaglioGiroContatore.getQueryDF(df)
    QueryPdrDettaglioGiroContatore.writeOnHive(pdrDettaglioGiroContatore)
  }

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df

  override val outputSchema: SchemaEnum = new SchemaEnum {}

  override def hdfsOutputPath: String = ""
}
