package it.eng.au.aggregatoreConsumiSbg.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, UdbAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.AggregatorTraitSbg
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object UdbAggregatorSbg extends AggregatorTraitSbg {
  override val baseNumber: String = "5"
  override val keyFields: List[String] = List(UdbAggregatorOutputSchema.PIVA_UDB.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pivaDistr.toString -> UdbAggregatorOutputSchema.PIVA_DISTR.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> UdbAggregatorOutputSchema.PIVA_UDB.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> UdbAggregatorOutputSchema.PIVA_UDD.toString,
    DailyConsumptionAggSchema.dtg.toString -> UdbAggregatorOutputSchema.DTG.toString,
    DailyConsumptionAggSchema.codRemi.toString -> UdbAggregatorOutputSchema.COD_REMI.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> UdbAggregatorOutputSchema.ID_REG_CLIM.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> UdbAggregatorOutputSchema.COD_PROF_PREL_STD.toString,
    DailyConsumptionAggSchema.treatment.toString -> UdbAggregatorOutputSchema.TRATTAMENTO.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> UdbAggregatorOutputSchema.TIPO_CLIENTE.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> UdbAggregatorOutputSchema.UNIT_MIS_PREL.toString
  )

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull and col(DailyConsumptionAggSchema.pivaUdb).isNotNull

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

}
