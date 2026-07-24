package it.eng.au.pubblicazioneRendiconti.controller

import it.eng.au.indennizziMisureGasCommon.schema.IndennizziRzg2Schema
import it.eng.au.pubblicazioneRendiconti.schema.Rzg2CsvOutputSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

import scala.collection.immutable.ListMap

object PubblicazioneIndennizzi extends PubblicazioneIndennizziTrait {
  override val rzg2Columns: ListMap[String, String] = ListMap(
    IndennizziRzg2Schema.csv_data.toString -> Rzg2CsvOutputSchema.DATA.toString,
    IndennizziRzg2Schema.csv_id_indennizzo.toString -> Rzg2CsvOutputSchema.ID_INDENNIZZO.toString,
    IndennizziRzg2Schema.csv_piva_id.toString -> Rzg2CsvOutputSchema.PIVA_ID.toString,
    IndennizziRzg2Schema.csv_rag_soc_id.toString -> Rzg2CsvOutputSchema.RAG_SOC_ID.toString,
    IndennizziRzg2Schema.csv_piva_udd.toString -> Rzg2CsvOutputSchema.PIVA_UDD.toString,
    IndennizziRzg2Schema.csv_rag_soc_udd.toString -> Rzg2CsvOutputSchema.RAG_SOC_UDD.toString,
    IndennizziRzg2Schema.csv_euro_om1.toString -> Rzg2CsvOutputSchema._EURO_SYMBOL_OM1_ID.toString,
    IndennizziRzg2Schema.csv_euro_om2.toString -> Rzg2CsvOutputSchema._EURO_SYMBOL_OM2_ID.toString,
    IndennizziRzg2Schema.csv_euro_om3.toString -> Rzg2CsvOutputSchema._EURO_SYMBOL_OM3_ID.toString
  )
  override val csvFields: List[String] = rzg2Columns.values.toList

  override val keyFields: ListMap[String, String] = ListMap(
    pivaID -> Rzg2CsvOutputSchema.PIVA_ID,
    pivaUdd -> Rzg2CsvOutputSchema.PIVA_UDD,
    annomese -> IndennizziRzg2Schema.anno_mese_competenza
  )

  override def getIndennizzi(df: DataFrame): DataFrame = {
    var aggDF = df
    rzg2Columns.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumn(fileName, col(dailyName).cast(StringType))
    })

    aggDF.selectExpr(rzg2Columns.values.toSeq.union(keyFields.values.toSeq).distinct: _*)
  }
}
