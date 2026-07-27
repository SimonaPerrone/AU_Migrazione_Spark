package it.eng.au.queryReport.query.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.pdr.PdrDettaglioUnicoSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.PdrDettaglioUnicoQuerySchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.immutable.ListMap

/** Esegue la query della pubblicazione definita in [[PdrDettaglioUnicoSbg]]. */
object QueryPdrDettaglioUnico extends QueryTrait with PdrDettaglioUnicoSbg {
  override val queryName = "pdrDettaglioUnico"
  override val tableName: String = Environment.getPdrDettaglioUnicoTableName

  val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> PdrDettaglioUnicoQuerySchema.pdr.toString,
    DailyConsumptionAggSchema.value.toString -> PdrDettaglioUnicoQuerySchema.prelievo.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> PdrDettaglioUnicoQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.dtg.toString -> PdrDettaglioUnicoQuerySchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> PdrDettaglioUnicoQuerySchema.cod_remi.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> PdrDettaglioUnicoQuerySchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> PdrDettaglioUnicoQuerySchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> PdrDettaglioUnicoQuerySchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> PdrDettaglioUnicoQuerySchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.session.toString -> PdrDettaglioUnicoQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> PdrDettaglioUnicoQuerySchema.annomese.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> PdrDettaglioUnicoQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> PdrDettaglioUnicoQuerySchema.piva_rdb.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> PdrDettaglioUnicoQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> PdrDettaglioUnicoQuerySchema.piva_it.toString
  )

  override val outputSchema: SchemaEnum = PdrDettaglioUnicoQuerySchema

  override def hdfsOutputPath: String = Environment.getQueryPdrDettUniHdfsPath

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
    col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  override def filteringAndSelect(df: DataFrame): DataFrame = {
    df.selectExpr(aggregatoColumns.values.toSeq: _*)
  }

  // Not needed
  override def getCsvFields(dfAggregato: DataFrame): List[String] = List()
  override val keyPiva1: String = ""
  override val keyPiva2: String = ""
  override val baseNumber: String = ""
  override val mainPiva: String = ""
}