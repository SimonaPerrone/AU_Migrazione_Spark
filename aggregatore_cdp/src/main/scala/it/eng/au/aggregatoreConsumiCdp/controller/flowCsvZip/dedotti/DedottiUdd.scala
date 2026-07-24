package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.dedotti

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits.Dedotti
import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object DedottiUdd extends Dedotti {
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
    OutputHiveSchema.causale.toString -> OutputCsvSchema.causale.toString,
    OutputHiveSchema.tipo_trasmissione.toString -> OutputCsvSchema.sessione.toString,
    OutputHiveSchema.anno_competenza.toString -> OutputCsvSchema.anno.toString
  )
  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(OutputCsvSchema.piva_udd.toString, OutputCsvSchema.piva_distr.toString))

  override def fileSpecificFilterExpression: Column = col(OutputCsvSchema.piva_udd).isNotNull && col(OutputCsvSchema.piva_distr).isNotNull

  override def getCsvOutputPath(baseName: String, mapKeys: Map[String, String], date: LocalDateTime, sessione: String, annoCompetenza: String, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKeys(mainPiva)
    val pivaNameFile = keyFields.reverse.map(mapKeys(_)).mkString("_")

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_CDP-${sessione}_${operationName}_${annoCompetenza}_${timestamp}_${counterCsv}.csv"
  }
}
