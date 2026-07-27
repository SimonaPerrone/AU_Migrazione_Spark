package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.elencoRemi
import it.eng.au.aggregatoreConsumiCommon.schema.{AggregatoTriplaElencoRemiSchema, DailyConsumptionAggSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object RdbElencoRemiDettaglioAggregatoTripla extends ElencoRemiDettaglioAggregatoTripla {
  override val baseNumber: String = "2"
  override val keyFields: List[String] = List(AggregatoTriplaElencoRemiSchema.piva_rdb)
  override val mainPiva: String = keyFields.head

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.codRemi.toString -> AggregatoTriplaElencoRemiSchema.codice_remi.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> AggregatoTriplaElencoRemiSchema.piva_rdb.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> AggregatoTriplaElencoRemiSchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> AggregatoTriplaElencoRemiSchema.piva_it.toString,
    DailyConsumptionAggSchema.annoMese.toString -> AggregatoTriplaElencoRemiSchema.annomese.toString
  )

  override val csvFields: List[String] = aggregatoColumns.values.toList.diff(List(AggregatoTriplaElencoRemiSchema.piva_rdb.toString, AggregatoTriplaElencoRemiSchema.annomese.toString))

  override def fileSpecificFilterExpression: Column = {
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
      col(DailyConsumptionAggSchema.pivaRdb).isNotNull
  }
}
