package it.eng.au.queryReport.query.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum
import it.eng.au.queryReport.query.traits.QueryTrait
import org.apache.spark.sql.DataFrame

object QueryDettaglioDeltaNegativo extends QueryTrait {
  override val queryName: String = "dettaglioDeltaNegativo"
  override val tableName: String = ""

  override def runQuery(df: DataFrame): Unit = {
    val validateFlow = getAndPrepareValidateFlow()

    val elencoFlussiDettaglioDeltaNegativo = QueryElencoFlussiDettaglioDeltaNegativo.getQueryDF(df, validateFlow)
    QueryElencoFlussiDettaglioDeltaNegativo.writeOnHive(elencoFlussiDettaglioDeltaNegativo)

    val pdrDettaglioDeltaNegativo = QueryPdrDettaglioDeltaNegativo.getQueryDF(df)
    QueryPdrDettaglioDeltaNegativo.writeOnHive(pdrDettaglioDeltaNegativo)
  }

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df

  override val outputSchema: SchemaEnum = new SchemaEnum {}

  override def hdfsOutputPath: String = ""
}
