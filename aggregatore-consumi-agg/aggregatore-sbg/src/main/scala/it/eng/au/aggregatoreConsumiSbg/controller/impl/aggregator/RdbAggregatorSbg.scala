package it.eng.au.aggregatoreConsumiSbg.controller.impl.aggregator

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, RdbAggregatorOutputSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.AggregatorTraitSbg
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

import scala.collection.immutable.ListMap

object RdbAggregatorSbg extends AggregatorTraitSbg {
  override val baseNumber: String = "2"
  override val keyFields: List[String] = List(RdbAggregatorOutputSchema.PIVA_RDB)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pivaRdb.toString -> RdbAggregatorOutputSchema.PIVA_RDB.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> RdbAggregatorOutputSchema.PIVA_IT.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> RdbAggregatorOutputSchema.PIVA_UDD.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> RdbAggregatorOutputSchema.PIVA_UDB.toString,
    DailyConsumptionAggSchema.dtg.toString -> RdbAggregatorOutputSchema.DTG.toString,
    DailyConsumptionAggSchema.codRemi.toString -> RdbAggregatorOutputSchema.COD_REMI.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> RdbAggregatorOutputSchema.ID_REG_CLIM.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> RdbAggregatorOutputSchema.COD_PROF_PREL_STD.toString,
    DailyConsumptionAggSchema.treatment.toString -> RdbAggregatorOutputSchema.TRATTAMENTO.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> RdbAggregatorOutputSchema.TIPO_CLIENTE.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> RdbAggregatorOutputSchema.UNIT_MIS_PREL.toString
  )

  override def fileSpecificFilterExpression: Column = {
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
      col(DailyConsumptionAggSchema.pivaRdb).isNotNull and
      (col(DailyConsumptionAggSchema.pivaIt) =!= lit("10238291008"))
  }

  override val csvFields: List[String] = (List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList).diff(List(RdbAggregatorOutputSchema.PIVA_RDB.toString))

  override val writeCsvHeader: Boolean = false
}
