package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits.dettaglioFlussi

import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema, ValidatedFlowsSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

trait DettaglioFlussiUdd extends DettaglioFlussi {
  override val baseName: String = "CDP1"
  override val keyFields: List[String] = List(OutputCsvSchema.piva_udd.toString, OutputCsvSchema.piva_distr.toString)
  override val mainPiva: String = OutputCsvSchema.piva_udd.toString
  override val secondaryPiva: String = OutputCsvSchema.piva_distr.toString

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    OutputHiveSchema.piva_udd.toString -> OutputCsvSchema.piva_udd.toString,
    OutputHiveSchema.cod_pdr.toString -> OutputCsvSchema.pdr.toString,
    OutputHiveSchema.prelievo_annuo_prev.toString -> OutputCsvSchema.prel_annuo_prev.toString,
    OutputHiveSchema.trattamento.toString -> OutputCsvSchema.trattamento.toString,
    ValidatedFlowsSchema.local_file.toString -> OutputCsvSchema.path_cloud.toString,
    OutputHiveSchema.anno_competenza.toString -> OutputCsvSchema.AT.toString,
    OutputHiveSchema.tipo_trasmissione.toString -> OutputCsvSchema.sessione.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(OutputCsvSchema.piva_udd.toString, OutputCsvSchema.sessione.toString))

  override def fileSpecificFilterExpression: Column = col(OutputCsvSchema.piva_udd).isNotNull

  // AU-694: richiesto che il nome del CSV abbia prima piva_distr e poi piva_udd
  // se viene richiesto di invertire le piva allora cancellare il metodo in modo che utilizzi quello di DettaglioFlussi
  override def getCsvOutputPath(baseName: String, mapKeys: Map[String, String], date: LocalDateTime, sessione: String, annoCompetenza: String, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaMain = mapKeys(mainPiva)
    val pivaSecondary = mapKeys(secondaryPiva)

    s"/${baseName}_$pivaMain/$year/$month/${pivaSecondary}_${pivaMain}_CDP-${sessione}_Elenco_Flussi_${annoCompetenza}_${timestamp}_${counterCsv}.csv"
  }

}
