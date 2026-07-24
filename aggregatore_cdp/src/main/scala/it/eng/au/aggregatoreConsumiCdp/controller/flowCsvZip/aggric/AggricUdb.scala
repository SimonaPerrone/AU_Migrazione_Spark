package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggric

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits.Aggric
import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object AggricUdb extends Aggric {
  override val baseName: String = "CDP3"
  override val keyFields: List[String] = List(OutputCsvSchema.piva_udb.toString, OutputCsvSchema.piva_udd.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    OutputHiveSchema.piva_udd.toString -> OutputCsvSchema.piva_udd.toString,
    OutputHiveSchema.piva_udb.toString -> OutputCsvSchema.piva_udb.toString,
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
  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(OutputCsvSchema.piva_udd.toString, OutputCsvSchema.piva_udb.toString, OutputCsvSchema.sessione.toString))

  override def fileSpecificFilterExpression: Column = col(OutputCsvSchema.piva_udb).isNotNull && col(OutputCsvSchema.piva_udd).isNotNull

}
