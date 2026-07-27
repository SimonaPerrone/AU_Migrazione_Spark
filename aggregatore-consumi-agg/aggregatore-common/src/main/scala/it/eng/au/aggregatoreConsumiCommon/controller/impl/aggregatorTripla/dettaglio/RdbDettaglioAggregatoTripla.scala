package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.dettaglio

import it.eng.au.aggregatoreConsumiCommon.schema.{AggregatoTriplaSchema, DailyConsumptionAggSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object RdbDettaglioAggregatoTripla extends PdrDettaglioAggregatoTripla {
  override val baseNumber: String = "2"
  override val keyFields: List[String] = List(AggregatoTriplaSchema.piva_rdb)
  override val mainPiva: String = keyFields.head

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.date.toString -> AggregatoTriplaSchema.data.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> AggregatoTriplaSchema.piva_rdb.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> AggregatoTriplaSchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> AggregatoTriplaSchema.piva_it.toString,
    DailyConsumptionAggSchema.codRemi.toString -> AggregatoTriplaSchema.codice_remi.toString,
    AggregatoTriplaSchema.consumo.toString -> AggregatoTriplaSchema.consumo.toString,
    DailyConsumptionAggSchema.annoMese.toString -> AggregatoTriplaSchema.annomese.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(AggregatoTriplaSchema.piva_rdb.toString, AggregatoTriplaSchema.annomese.toString))

  override def fileSpecificFilterExpression: Column = {
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
      col(DailyConsumptionAggSchema.pivaRdb).isNotNull
  }
}
