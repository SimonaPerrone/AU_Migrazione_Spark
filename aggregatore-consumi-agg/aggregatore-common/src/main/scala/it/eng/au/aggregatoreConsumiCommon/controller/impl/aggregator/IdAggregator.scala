package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.controller.traits.AggregatorTrait
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IdAggregatorOutputSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object IdAggregator extends AggregatorTrait {
  override val baseNumber: String = "4"
  override val keyFields: List[String] = List(IdAggregatorOutputSchema.PIVA_DISTR.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pivaDistr.toString -> IdAggregatorOutputSchema.PIVA_DISTR.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> IdAggregatorOutputSchema.PIVA_UDD.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> IdAggregatorOutputSchema.PIVA_UDB.toString,
    DailyConsumptionAggSchema.dtg.toString -> IdAggregatorOutputSchema.DTG.toString,
    DailyConsumptionAggSchema.codRemi.toString -> IdAggregatorOutputSchema.COD_REMI.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> IdAggregatorOutputSchema.ID_REG_CLIM.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> IdAggregatorOutputSchema.COD_PROF_PREL_STD.toString,
    DailyConsumptionAggSchema.treatment.toString -> IdAggregatorOutputSchema.TRATTAMENTO.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> IdAggregatorOutputSchema.TIPO_CLIENTE.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> IdAggregatorOutputSchema.UNIT_MIS_PREL.toString
  )

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

}
