package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.traits.AggregatorTraitSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.AggregatoQuerySchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

/** Esegue la query della pubblicazione definita in [[AggregatorTraitSbg]]. */
object QueryAggregato extends QueryTrait with AggregatorTraitSbg {
  override val queryName: String = "aggregato"
  override def tableName: String = Environment.getAggregatoTableName

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> AggregatoQuerySchema.pdr.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> AggregatoQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> AggregatoQuerySchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> AggregatoQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> AggregatoQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> AggregatoQuerySchema.piva_rdb.toString,
    DailyConsumptionAggSchema.dtg.toString -> AggregatoQuerySchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> AggregatoQuerySchema.cod_remi.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> AggregatoQuerySchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> AggregatoQuerySchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> AggregatoQuerySchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> AggregatoQuerySchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> AggregatoQuerySchema.unit_mis_prel.toString
  )

  override def fileSpecificFilterExpression: Column =
    col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
    col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  override def hdfsOutputPath: String = Environment.getQueryAggregatoHdfsPath

  override val dataValColName: String = AggregatoQuerySchema.data_val
  override val pivotPrefix = "prelievo_giorn_"
  override val outputSchema: SchemaEnum = AggregatoQuerySchema

  //Fields not needed
  override val baseNumber: String = ""
  override val csvFields: List[String] = List()
  override val keyFields: List[String] = List()
}