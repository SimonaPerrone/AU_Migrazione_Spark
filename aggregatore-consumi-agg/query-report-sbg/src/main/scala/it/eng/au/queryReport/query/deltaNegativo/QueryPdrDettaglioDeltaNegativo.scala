package it.eng.au.queryReport.query.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.pdr.PdrDettaglioDeltaNegativoSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.DeltaNegativoQuerySchema
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, dayofmonth, lit}

import scala.collection.immutable.ListMap

object QueryPdrDettaglioDeltaNegativo extends QueryTrait with PdrDettaglioDeltaNegativoSbg {
  override val queryName = "pdrDettaglioDeltaNegativo"
  override val tableName: String = Environment.getPdrDettaglioDeltaNegativoTableName

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.date.toString -> DeltaNegativoQuerySchema.data,
    DailyConsumptionAggSchema.pdr.toString -> DeltaNegativoQuerySchema.pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> DeltaNegativoQuerySchema.piva_distr,
    DailyConsumptionAggSchema.pivaIt.toString -> DeltaNegativoQuerySchema.piva_it,
    DailyConsumptionAggSchema.pivaUdd.toString -> DeltaNegativoQuerySchema.piva_udd,
    DailyConsumptionAggSchema.pivaUdb.toString -> DeltaNegativoQuerySchema.piva_udb,
    DailyConsumptionAggSchema.pivaRdb.toString -> DeltaNegativoQuerySchema.piva_rdb,
    DailyConsumptionAggSchema.dtg.toString -> DeltaNegativoQuerySchema.dtg,
    DailyConsumptionAggSchema.codRemi.toString -> DeltaNegativoQuerySchema.cod_remi,
    DailyConsumptionAggSchema.ca.toString -> DeltaNegativoQuerySchema.prel_annuo_prev,
    DailyConsumptionAggSchema.treatment.toString -> DeltaNegativoQuerySchema.trattamento,
    DailyConsumptionAggSchema.tipoCliente.toString -> DeltaNegativoQuerySchema.tipo_cliente,
    DailyConsumptionAggSchema.unitMisPrel.toString -> DeltaNegativoQuerySchema.un_mis_prel,
    DeltaNegativoQuerySchema.GIORN_DN.toString -> DeltaNegativoQuerySchema.GIORN_DN,
    DeltaNegativoQuerySchema.PRELIEVO_GIORNO_DN.toString -> DeltaNegativoQuerySchema.PRELIEVO_GIORNO_DN,
    DailyConsumptionAggSchema.annoMese.toString -> DeltaNegativoQuerySchema.annomese
  )

  override def getAggregato(df: DataFrame): DataFrame = {
    var finalDF = df
      .filter(col(DailyConsumptionAggSchema.errorCode) === 12)
      .withColumn(DeltaNegativoQuerySchema.GIORN_DN, dayofmonth(col(DailyConsumptionAggSchema.date)))
      .withColumn(DeltaNegativoQuerySchema.PRELIEVO_GIORNO_DN, lit(0.0))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF.distinct
  }

  override val outputSchema: SchemaEnum = DeltaNegativoQuerySchema

  override def hdfsOutputPath: String = Environment.getQueryPdrDettaglioDeltaNegativoHdfsPath

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
