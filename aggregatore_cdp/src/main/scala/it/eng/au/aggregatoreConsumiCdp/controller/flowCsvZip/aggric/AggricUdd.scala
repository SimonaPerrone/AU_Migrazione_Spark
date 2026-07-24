package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggric

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits.Aggric
import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

import java.util.Properties
import scala.collection.immutable.ListMap

object AggricUdd extends Aggric {
  override val baseName: String = "CDP1"
  override val keyFields: List[String] = List(OutputCsvSchema.piva_udd.toString, OutputCsvSchema.piva_distr.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    OutputHiveSchema.piva_udd.toString -> OutputCsvSchema.piva_udd.toString,
    OutputHiveSchema.piva_distr.toString -> OutputCsvSchema.piva_distr.toString,
    OutputHiveSchema.cod_pdr.toString -> OutputCsvSchema.cod_pdr.toString,
    OutputHiveSchema.cod_remi.toString -> OutputCsvSchema.cod_remi.toString,
    OutputHiveSchema.cat_uso.toString -> OutputCsvSchema.cat_uso.toString,
    OutputHiveSchema.classe_prelievo.toString -> OutputCsvSchema.classe_prelievo.toString,
    OutputHiveSchema.zona_climatica.toString -> OutputCsvSchema.zona_climatica.toString,
    OutputHiveSchema.id_reg_clim.toString -> OutputCsvSchema.id_reg_clim.toString,
    OutputHiveSchema.cod_prof_prel_std.toString -> OutputCsvSchema.cod_prof_prel_std.toString,
    OutputHiveSchema.prelievo_annuo_prev.toString -> OutputCsvSchema.prelievo_annuo_prev.toString,
    OutputHiveSchema.trattamento.toString -> OutputCsvSchema.trattamento.toString,
    OutputHiveSchema.tipo_trasmissione.toString -> OutputCsvSchema.sessione.toString,
    OutputHiveSchema.data_decorrenza.toString -> OutputCsvSchema.data_decorrenza.toString
  )
  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(OutputCsvSchema.piva_udd.toString, OutputCsvSchema.piva_distr.toString, OutputCsvSchema.sessione.toString))

  override def fileSpecificFilterExpression: Column = col(OutputCsvSchema.piva_udd).isNotNull && col(OutputCsvSchema.piva_distr).isNotNull

  override def splitUddSwitching(df: DataFrame): DataFrame = {
    df
      .withColumn(OutputHiveSchema.udd_oggetto_swithcing,
        explode(split(
          when(col(OutputHiveSchema.udd_oggetto_swithcing).isNotNull and col(OutputHiveSchema.udd_oggetto_swithcing).notEqual(""), concat(col(OutputHiveSchema.udd_oggetto_swithcing), lit(";"), col(OutputHiveSchema.piva_udd)))
            .otherwise(col(OutputHiveSchema.piva_udd))
          , ";")))
      .drop(col(OutputCsvSchema.piva_udd))
      .withColumnRenamed(OutputHiveSchema.udd_oggetto_swithcing, OutputCsvSchema.piva_udd)
      .distinct()
  }
}
