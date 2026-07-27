package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.pdr

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, EsclusiOutputSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object UddPdrEsclusiDettaglio extends PdrDettaglioEsclusi {
  override val baseNumber: String = "1"
  override val keyPiva1: String = EsclusiOutputSchema.piva_udd
  override val keyPiva2: String = EsclusiOutputSchema.piva_distr
  override val mainPiva: String = keyPiva1
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> EsclusiOutputSchema.cod_pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> EsclusiOutputSchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> EsclusiOutputSchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> EsclusiOutputSchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> EsclusiOutputSchema.piva_udb.toString,
    DailyConsumptionAggSchema.dtg.toString -> EsclusiOutputSchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> EsclusiOutputSchema.cod_remi.toString,
    DailyConsumptionAggSchema.ca.toString -> EsclusiOutputSchema.prel_annuo_prev.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> EsclusiOutputSchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> EsclusiOutputSchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> EsclusiOutputSchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> EsclusiOutputSchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.causale.toString -> EsclusiOutputSchema.causale.toString,
    DailyConsumptionAggSchema.valuef3.toString -> EsclusiOutputSchema.prelievo_aggregato.toString,
    DailyConsumptionAggSchema.session.toString -> EsclusiOutputSchema.Sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> EsclusiOutputSchema.annoMese.toString
  )

  override def fileSpecificFilterExpression: Column = lit(true)

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva2}_${piva1}_AGG_${operationName}_${annomese}_${timestamp}_${counterCsv}.csv"
  }

  override val csvFields: List[String] = List(dataValColName) ++ aggregatoColumns.values
  override val header: String = ""
}
