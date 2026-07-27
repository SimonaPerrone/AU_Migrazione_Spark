package it.eng.au.queryReport.query.giroContatore

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.pdr.PdrDettaglioGiroContatoreSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.GiroContatoreQuerySchema
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, dayofmonth}

import scala.collection.immutable.ListMap

object QueryPdrDettaglioGiroContatore extends QueryTrait with PdrDettaglioGiroContatoreSbg {
  override val queryName = "pdrDettaglioGiroContatore"
  override val tableName: String = Environment.getPdrDettaglioGiroContatoreTableName

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.date.toString -> GiroContatoreQuerySchema.data,
    DailyConsumptionAggSchema.pdr.toString -> GiroContatoreQuerySchema.pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> GiroContatoreQuerySchema.piva_distr,
    DailyConsumptionAggSchema.pivaIt.toString -> GiroContatoreQuerySchema.piva_it,
    DailyConsumptionAggSchema.pivaUdd.toString -> GiroContatoreQuerySchema.piva_udd,
    DailyConsumptionAggSchema.pivaUdb.toString -> GiroContatoreQuerySchema.piva_udb,
    DailyConsumptionAggSchema.pivaRdb.toString -> GiroContatoreQuerySchema.piva_rdb,
    DailyConsumptionAggSchema.dtg.toString -> GiroContatoreQuerySchema.dtg,
    DailyConsumptionAggSchema.codRemi.toString -> GiroContatoreQuerySchema.cod_remi,
    DailyConsumptionAggSchema.ca.toString -> GiroContatoreQuerySchema.prel_annuo_prev,
    DailyConsumptionAggSchema.treatment.toString -> GiroContatoreQuerySchema.trattamento,
    DailyConsumptionAggSchema.tipoCliente.toString -> GiroContatoreQuerySchema.tipo_cliente,
    DailyConsumptionAggSchema.unitMisPrel.toString -> GiroContatoreQuerySchema.un_mis_prel,
    GiroContatoreQuerySchema.GIORN_GC.toString -> GiroContatoreQuerySchema.GIORN_GC,
    GiroContatoreQuerySchema.PRELIEVO_GIORNO_GC.toString -> GiroContatoreQuerySchema.PRELIEVO_GIORNO_GC,
    DailyConsumptionAggSchema.annoMese.toString -> GiroContatoreQuerySchema.annomese
  )

  override def getAggregato(df: DataFrame): DataFrame = {
    var finalDF = df.filter(col(DailyConsumptionAggSchema.idFormula) === 6)
      .withColumn(GiroContatoreQuerySchema.GIORN_GC, dayofmonth(col(DailyConsumptionAggSchema.date)))
      .withColumn(GiroContatoreQuerySchema.PRELIEVO_GIORNO_GC, col(DailyConsumptionAggSchema.value))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF.distinct
  }

  override val outputSchema: SchemaEnum = GiroContatoreQuerySchema

  override def hdfsOutputPath: String = Environment.getQueryPdrDettaglioGiroContatoreHdfsPath

  override def fileSpecificFilterExpression: Column =
    col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
      col(DailyConsumptionAggSchema.pivaIt).isNotNull and
      col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
      col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
      col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  // Not needed
  override val csvFields: List[String] = List()
  override val baseNumber: String = ""
  override val mainPiva: String = ""
  override val keyPiva1: String = ""
  override val keyPiva2: String = ""
}
