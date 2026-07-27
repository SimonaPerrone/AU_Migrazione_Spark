package it.eng.au.queryReport.query.dettaglioIncoerenti

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr.PdrDettaglioIncoerentiSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.PdrDettaglioIncoerentiQuerySchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

/** Esegue la query della pubblicazione definita in [[PdrDettaglioIncoerentiSbg]]. */
object QueryPdrDettaglioIncoerenti extends QueryTrait with PdrDettaglioIncoerentiSbg {
  override val queryName = "pdrDettaglioIncoerenti"
  override val tableName: String = Environment.getPdrDettaglioIncoerentiTableName

  val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> PdrDettaglioIncoerentiQuerySchema.pdr.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> PdrDettaglioIncoerentiQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.dtg.toString -> PdrDettaglioIncoerentiQuerySchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> PdrDettaglioIncoerentiQuerySchema.cod_remi.toString,
    DailyConsumptionAggSchema.ca.toString -> PdrDettaglioIncoerentiQuerySchema.prel_annuo_prev.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> PdrDettaglioIncoerentiQuerySchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> PdrDettaglioIncoerentiQuerySchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> PdrDettaglioIncoerentiQuerySchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> PdrDettaglioIncoerentiQuerySchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> PdrDettaglioIncoerentiQuerySchema.unit_mis_prel.toString,
    DailyConsumptionAggSchema.classeMisuratore.toString -> PdrDettaglioIncoerentiQuerySchema.classe_gruppo_mis.toString,
    prelievoAggregato -> PdrDettaglioIncoerentiQuerySchema.prelievo_aggregato.toString,
    anomalousDays -> PdrDettaglioIncoerentiQuerySchema.giorno_sterilizzato.toString,
    DailyConsumptionAggSchema.session.toString -> PdrDettaglioIncoerentiQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> PdrDettaglioIncoerentiQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> PdrDettaglioIncoerentiQuerySchema.piva_rdb.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> PdrDettaglioIncoerentiQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> PdrDettaglioIncoerentiQuerySchema.piva_it.toString
  )

  override val outputSchema: SchemaEnum = PdrDettaglioIncoerentiQuerySchema

  override def hdfsOutputPath: String = Environment.getQueryPdrDettaglioIncoerentiHdfsPath

  override def fileSpecificFilterExpression: Column =
    col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
    col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  // Not needed
  override val keyFields: List[String] = List()
  override val csvFields: List[String] = List()
  override val baseNumber: String = ""
  override val mainPiva: String = ""
}