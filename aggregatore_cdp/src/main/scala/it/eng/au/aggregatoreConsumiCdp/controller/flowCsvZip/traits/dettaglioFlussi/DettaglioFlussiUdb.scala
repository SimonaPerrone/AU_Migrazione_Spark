package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits.dettaglioFlussi

import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema, ValidatedFlowsSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

trait DettaglioFlussiUdb extends DettaglioFlussi {
  override val baseName: String = "CDP3"
  override val keyFields: List[String] = List(OutputCsvSchema.piva_udb.toString, OutputCsvSchema.piva_udd.toString)
  override val mainPiva: String = OutputCsvSchema.piva_udb.toString
  override val secondaryPiva: String = OutputCsvSchema.piva_udd.toString

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    OutputHiveSchema.piva_udb.toString -> OutputCsvSchema.piva_udb.toString,
    OutputHiveSchema.cod_pdr.toString -> OutputCsvSchema.pdr.toString,
    OutputHiveSchema.prelievo_annuo_prev.toString -> OutputCsvSchema.prel_annuo_prev.toString,
    OutputHiveSchema.trattamento.toString -> OutputCsvSchema.trattamento.toString,
    ValidatedFlowsSchema.local_file.toString -> OutputCsvSchema.path_cloud.toString,
    OutputHiveSchema.anno_competenza.toString -> OutputCsvSchema.AT.toString,
    OutputHiveSchema.tipo_trasmissione.toString -> OutputCsvSchema.sessione.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(OutputCsvSchema.piva_udb.toString, OutputCsvSchema.sessione.toString))

  override def fileSpecificFilterExpression: Column = col(OutputCsvSchema.piva_udb).isNotNull

}
