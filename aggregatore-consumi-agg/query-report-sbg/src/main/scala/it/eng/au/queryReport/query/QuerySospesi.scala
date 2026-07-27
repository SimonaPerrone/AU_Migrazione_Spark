package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, RcugasPdrSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.{RcugasSospensioniSchema, SospesiQuerySchema}
import org.apache.spark.sql.functions.{coalesce, col, lit, not, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.sql.{Date, Timestamp}
import scala.collection.immutable.ListMap

/** Estrae il perimetro dei PdR sospesi. */
object QuerySospesi extends QueryTrait {
  override val queryName: String = "sospesi"
  override def tableName: String = Environment.getSospesiTableName

  val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> SospesiQuerySchema.pdr,
    DailyConsumptionAggSchema.pivaDistr.toString -> SospesiQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> SospesiQuerySchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> SospesiQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> SospesiQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> SospesiQuerySchema.piva_rdb.toString,
    DailyConsumptionAggSchema.dtg.toString -> SospesiQuerySchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> SospesiQuerySchema.cod_remi.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> SospesiQuerySchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> SospesiQuerySchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> SospesiQuerySchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> SospesiQuerySchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.session.toString -> SospesiQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> SospesiQuerySchema.annomese.toString
  )

  val rcugasColumns: ListMap[String, String] = ListMap(
  RcugasSospensioniSchema.d_data_inizio_sosp.toString -> SospesiQuerySchema.data_inizio_sosp.toString,
  RcugasSospensioniSchema.d_data_revoca_sosp.toString -> SospesiQuerySchema.data_revoca_sosp.toString,
  RcugasSospensioniSchema.t_cod_causale_sospensione.toString -> SospesiQuerySchema.motivazione_sosp.toString
  )

  override val outputSchema: SchemaEnum = SospesiQuerySchema
  override def hdfsOutputPath: String = Environment.getQuerySospesiHdfsPath

  override def getQueryDF(df: DataFrame): DataFrame = {
    val annoMese = DateTime.parse(Environment.getYear, DateTimeFormat.forPattern("yyyyMM"))
    val startDate = annoMese.toString("yyyy-MM-dd") //the day is already set to 1
    val endDate = annoMese.plusMonths(1).toString("yyyy-MM-dd")

    val startDateColumn = to_timestamp(lit(startDate))
    val endDateColumn = to_timestamp(lit(endDate))

    val rcugasSospensioni = getRcugasSospensioni
    val rcugasPdr = getRcugasPdr

    getAggregato(df, rcugasSospensioni, rcugasPdr, startDateColumn, endDateColumn)
      .withColumn("dailyconsumption_executionid", lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn("executionid", lit(Timestamp.valueOf(Environment.getDateRun).getTime))
      .selectExpr(outputSchema.getValues: _*)
  }

  def getAggregato(df: DataFrame, rcugasSospensioni: DataFrame, rcugasPdr: DataFrame, startDateColumn: Column, endDateColumn: Column): DataFrame = {
    val sospensioniCodPdr = rcugasSospensioni
      .withColumn(RcugasSospensioniSchema.d_data_inizio_sosp, to_timestamp(coalesce(col(RcugasSospensioniSchema.d_data_inizio_sosp), lit("1970-01-01 00:00:00.0"))))
      .withColumn(RcugasSospensioniSchema.d_data_revoca_sosp, to_timestamp(coalesce(col(RcugasSospensioniSchema.d_data_revoca_sosp), lit("2100-12-31 00:00:00.0"))))
      .where(not(
        col(RcugasSospensioniSchema.d_data_inizio_sosp) >= endDateColumn or
          col(RcugasSospensioniSchema.d_data_revoca_sosp) < startDateColumn))
      .join(rcugasPdr, Seq("n_id_pdr"), "left")
      .drop(rcugasPdr(RcugasPdrSchema.n_id_pdr))
      .drop(rcugasSospensioni(RcugasSospensioniSchema.n_id_pdr))
      .distinct()

    var aggDF = df
      .selectExpr(aggregatoColumns.keys.toList: _*)
      .distinct()
      .join(sospensioniCodPdr, col(DailyConsumptionAggSchema.pdr) === col(RcugasPdrSchema.t_codice_pdr))
      .drop(col(RcugasPdrSchema.t_codice_pdr))

    (aggregatoColumns ++ rcugasColumns).foreach({ case (currentName, outputName) =>
      aggDF = aggDF.withColumnRenamed(currentName, outputName)
    })

    aggDF
  }

  def getRcugasSospensioni: DataFrame = {
    Environment.sqlContext.table(Environment.getRcugasSospensioniTableName)
      .selectExpr(RcugasSospensioniSchema.getValues: _*)
  }

  def getRcugasPdr: DataFrame = {
    Environment.sqlContext.table(Environment.getRcugasPdrTableName)
      .selectExpr(RcugasPdrSchema.getValues: _*)
  }

  //not needed
  override def getAggregato(df: DataFrame): DataFrame = df
}