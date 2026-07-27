package it.eng.au.aggregatoreConsumiSbg.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, ItAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.AggregatorTraitSbg
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object ItAggregatorSbg extends AggregatorTraitSbg {
  override val baseNumber: String = "3"
  override val keyFields: List[String] = List(ItAggregatorOutputSchema.PIVA_IT.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pivaIt.toString -> ItAggregatorOutputSchema.PIVA_IT.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> ItAggregatorOutputSchema.PIVA_UDD.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> ItAggregatorOutputSchema.PIVA_UDB.toString,
    DailyConsumptionAggSchema.dtg.toString -> ItAggregatorOutputSchema.DTG.toString,
    DailyConsumptionAggSchema.codRemi.toString -> ItAggregatorOutputSchema.COD_REMI.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> ItAggregatorOutputSchema.ID_REG_CLIM.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> ItAggregatorOutputSchema.COD_PROF_PREL_STD.toString,
    DailyConsumptionAggSchema.treatment.toString -> ItAggregatorOutputSchema.TRATTAMENTO.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> ItAggregatorOutputSchema.TIPO_CLIENTE.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> ItAggregatorOutputSchema.UNIT_MIS_PREL.toString
  )

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaIt).isNotNull

  override val writeCsvHeader: Boolean = false

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

}
