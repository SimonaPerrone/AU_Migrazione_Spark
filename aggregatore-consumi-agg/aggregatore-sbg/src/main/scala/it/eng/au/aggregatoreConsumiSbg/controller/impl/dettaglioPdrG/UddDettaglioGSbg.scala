package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioPdrG

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioGOutputSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.DettaglioGTraitSbg

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object UddDettaglioGSbg extends DettaglioGTraitSbg {
  override val baseNumber: String = "1"
  override val keyFields: List[String] = List(DettaglioGOutputSchema.piva_distr.toString, DettaglioGOutputSchema.piva_udd.toString)
  //CR 04/08/2022: differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd, but path and zip names should be with pivaUdd as before
  override val mainPiva: String = keyFields.last
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> DettaglioGOutputSchema.cod_pdr.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> DettaglioGOutputSchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> DettaglioGOutputSchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> DettaglioGOutputSchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> DettaglioGOutputSchema.piva_udb.toString,
    DailyConsumptionAggSchema.dtg.toString -> DettaglioGOutputSchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> DettaglioGOutputSchema.cod_remi.toString,
    DailyConsumptionAggSchema.ca.toString -> DettaglioGOutputSchema.prel_annuo_prev.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> DettaglioGOutputSchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> DettaglioGOutputSchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> DettaglioGOutputSchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> DettaglioGOutputSchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> DettaglioGOutputSchema.un_mis_prel.toString
  )

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

  //CR 04/08/2022: differently from AGG, SBG1 csv name should start with pivaDistr_pivaUdd, but path and zip names should be with pivaUdd as before
  override def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.last)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    //es SBG1_0123456789/2022/04/0123456789_SBG_202204_20220428105421_1.csv
    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${publicationType}_${annoMese}_${timestamp}_${counterCsv}.csv"
  }
}
