package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.traits.{IncoerentiConfNoConfTraitSbg, IncoerentiTraitSbg}
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.IncoerentiQuerySchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

/** Esegue la query della pubblicazione definita in [[IncoerentiTraitSbg]]. */
object QueryIncoerenti extends QueryTrait with IncoerentiTraitSbg {
  override val queryName: String = "incoerenti"
  override def tableName: String = Environment.getIncoerentiTableName

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> IncoerentiQuerySchema.pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> IncoerentiQuerySchema.piva_distr,
    DailyConsumptionAggSchema.pivaIt.toString -> IncoerentiQuerySchema.piva_it,
    DailyConsumptionAggSchema.pivaUdd.toString -> IncoerentiQuerySchema.piva_udd,
    DailyConsumptionAggSchema.pivaUdb.toString -> IncoerentiQuerySchema.piva_udb,
    DailyConsumptionAggSchema.dtg.toString -> IncoerentiQuerySchema.dtg,
    DailyConsumptionAggSchema.codRemi.toString -> IncoerentiQuerySchema.cod_remi,
    DailyConsumptionAggSchema.ca.toString -> IncoerentiQuerySchema.prel_annuo_prev,
    DailyConsumptionAggSchema.idRegClim.toString -> IncoerentiQuerySchema.id_reg_clim,
    DailyConsumptionAggSchema.codProfStd.toString -> IncoerentiQuerySchema.cod_prof_prel_std,
    DailyConsumptionAggSchema.treatment.toString -> IncoerentiQuerySchema.trattamento,
    DailyConsumptionAggSchema.tipoCliente.toString -> IncoerentiQuerySchema.tipo_cliente,
    DailyConsumptionAggSchema.unitMisPrel.toString -> IncoerentiQuerySchema.unit_mis_prel,
    DailyConsumptionAggSchema.pivaRdb.toString -> IncoerentiQuerySchema.piva_rdb.toString
  )

  override def fileSpecificFilterExpression: Column =
    col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
    col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  override def hdfsOutputPath: String = Environment.getQueryIncoerentiHdfsPath

  override val outputSchema: SchemaEnum = IncoerentiQuerySchema

  //Fields not needed
  override val baseNumber: String = ""
  override val csvFields: List[String] = List()
  override val keyFields: List[String] = List()
  override val incoerentiConfNoConf: IncoerentiConfNoConfTraitSbg = null
}