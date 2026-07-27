package it.eng.au.queryReport.query.esclusi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.pdr.PdrDettaglioEsclusiSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.EsclusiQuerySchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.collection.immutable.ListMap

object QueryPdrDettaglioEsclusi extends QueryTrait with PdrDettaglioEsclusiSbg {
  override val queryName: String = "esclusi"
  override def tableName: String = Environment.getEsclusiTableName

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> EsclusiQuerySchema.pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> EsclusiQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> EsclusiQuerySchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> EsclusiQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> EsclusiQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> EsclusiQuerySchema.piva_rdb.toString,
    DailyConsumptionAggSchema.dtg.toString -> EsclusiQuerySchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> EsclusiQuerySchema.cod_remi.toString,
    DailyConsumptionAggSchema.ca.toString -> EsclusiQuerySchema.prel_annuo_prev.toString,
    DailyConsumptionAggSchema.valuef3.toString -> EsclusiQuerySchema.prelievo_aggregato.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> EsclusiQuerySchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> EsclusiQuerySchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> EsclusiQuerySchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> EsclusiQuerySchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.causale.toString -> EsclusiQuerySchema.motivazione_esclusione.toString,
    DailyConsumptionAggSchema.session.toString -> EsclusiQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> EsclusiQuerySchema.annomese.toString
  )

  override def fileSpecificFilterExpression: Column =
    col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
    col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  override def hdfsOutputPath: String = Environment.getQueryEsclusiHdfsPath

  override val outputSchema: SchemaEnum = EsclusiQuerySchema

  //Fields not needed
  override val baseNumber: String = ""
  override val csvFields: List[String] = List()
  override val keyPiva1: String = ""
  override val keyPiva2: String = ""
  override val mainPiva: String = ""
  override val header: String = ""
}