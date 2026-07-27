package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.controller.traits.AggregatorTrait
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, UdbAggregatorOutputSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object UdbAggregator extends AggregatorTrait {
  override val baseNumber: String = "5"
  override val keyFields: List[String] = List(UdbAggregatorOutputSchema.PIVA_UDB.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pivaDistr.toString -> UdbAggregatorOutputSchema.PIVA_DISTR.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> UdbAggregatorOutputSchema.PIVA_UDD,
    DailyConsumptionAggSchema.pivaUdb.toString -> UdbAggregatorOutputSchema.PIVA_UDB.toString,
    DailyConsumptionAggSchema.dtg.toString -> UdbAggregatorOutputSchema.DTG,
    DailyConsumptionAggSchema.codRemi.toString -> UdbAggregatorOutputSchema.COD_REMI,
    DailyConsumptionAggSchema.idRegClim.toString -> UdbAggregatorOutputSchema.ID_REG_CLIM,
    DailyConsumptionAggSchema.codProfStd.toString -> UdbAggregatorOutputSchema.COD_PROF_PREL_STD,
    DailyConsumptionAggSchema.treatment.toString -> UdbAggregatorOutputSchema.TRATTAMENTO,
    DailyConsumptionAggSchema.tipoCliente.toString -> UdbAggregatorOutputSchema.TIPO_CLIENTE,
    DailyConsumptionAggSchema.unitMisPrel.toString -> UdbAggregatorOutputSchema.UNIT_MIS_PREL
  )

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull and col(DailyConsumptionAggSchema.pivaUdb).isNotNull

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

}
