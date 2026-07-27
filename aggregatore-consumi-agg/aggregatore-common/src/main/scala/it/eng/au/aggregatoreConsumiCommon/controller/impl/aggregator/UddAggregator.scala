package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.controller.traits.AggregatorTrait
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, UddAggregatorOutputSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object UddAggregator extends AggregatorTrait {
  override val baseNumber: String = "1"
  override val keyFields: List[String] = List(UddAggregatorOutputSchema.PIVA_UDD.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pivaDistr.toString -> UddAggregatorOutputSchema.PIVA_DISTR.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> UddAggregatorOutputSchema.PIVA_UDD.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> UddAggregatorOutputSchema.PIVA_UDB.toString,
    DailyConsumptionAggSchema.dtg.toString -> UddAggregatorOutputSchema.DTG.toString,
    DailyConsumptionAggSchema.codRemi.toString -> UddAggregatorOutputSchema.COD_REMI.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> UddAggregatorOutputSchema.ID_REG_CLIM.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> UddAggregatorOutputSchema.COD_PROF_PREL_STD.toString,
    DailyConsumptionAggSchema.treatment.toString -> UddAggregatorOutputSchema.TRATTAMENTO.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> UddAggregatorOutputSchema.TIPO_CLIENTE.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> UddAggregatorOutputSchema.UNIT_MIS_PREL.toString
  )

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList
}