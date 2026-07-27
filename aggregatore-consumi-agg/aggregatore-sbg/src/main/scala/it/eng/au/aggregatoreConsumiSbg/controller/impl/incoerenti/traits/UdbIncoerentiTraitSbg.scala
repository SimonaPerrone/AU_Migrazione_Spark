package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerenti.traits

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IncoerentiOutputSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerenti.confNoConf.{RdbIncoerentiConfNoConf, UdbIncoerentiConfNoConf}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.{IncoerentiConfNoConfTraitSbg, IncoerentiTraitSbg}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

trait UdbIncoerentiTraitSbg extends IncoerentiTraitSbg {
  override val baseNumber: String = "5"
  override val incoerentiConfNoConf: IncoerentiConfNoConfTraitSbg = UdbIncoerentiConfNoConf
  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdb).isNotNull

  override val keyFields: List[String] = List(IncoerentiOutputSchema.piva_udb)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> IncoerentiOutputSchema.cod_pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> IncoerentiOutputSchema.piva_distr,
    DailyConsumptionAggSchema.pivaIt.toString -> IncoerentiOutputSchema.piva_it,
    DailyConsumptionAggSchema.pivaUdd.toString -> IncoerentiOutputSchema.piva_udd,
    DailyConsumptionAggSchema.pivaUdb.toString -> IncoerentiOutputSchema.piva_udb,
    DailyConsumptionAggSchema.dtg.toString -> IncoerentiOutputSchema.dtg,
    DailyConsumptionAggSchema.codRemi.toString -> IncoerentiOutputSchema.cod_remi,
    DailyConsumptionAggSchema.ca.toString -> IncoerentiOutputSchema.prel_annuo_prev,
    DailyConsumptionAggSchema.idRegClim.toString -> IncoerentiOutputSchema.id_reg_clim,
    DailyConsumptionAggSchema.codProfStd.toString -> IncoerentiOutputSchema.cod_prof_prel_std,
    DailyConsumptionAggSchema.treatment.toString -> IncoerentiOutputSchema.trattamento,
    DailyConsumptionAggSchema.tipoCliente.toString -> IncoerentiOutputSchema.tipo_cliente,
    DailyConsumptionAggSchema.unitMisPrel.toString -> IncoerentiOutputSchema.un_mis_prel
  )

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

}
