package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioPdrG

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DettaglioGOutputSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.DettaglioGTraitSbg

import scala.collection.immutable.ListMap

object IdDettaglioGSbg extends DettaglioGTraitSbg {
  override val baseNumber: String = "4"
  override val keyFields: List[String] = List(DettaglioGOutputSchema.piva_distr.toString, DettaglioGOutputSchema.piva_udd.toString)
  override val mainPiva: String = keyFields.head
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

}
