package it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg

import it.eng.au.ccgPubblicazione.schema.aggsbg.AggConsumptionRequestRunnableSchema
import it.eng.au.ccgPubblicazione.schema.aggsbg.output.SbgConsumiIncoerentiCOutputSchema
import it.eng.au.ccgPubblicazione.utility.Constants.INCOERENTIDETTAGLIO
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}

import scala.collection.immutable.ListMap

trait AggSbgIncoerentiC extends AggSbgPdrElencoFlussi {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  override val pivotPrefix = "prelievo_non_sterilizzato_giorn_"
  override val headerCsvConsumi: List[String] = SbgConsumiIncoerentiCOutputSchema.getValues ::: (1 to 31).map(pivotPrefix + _).toList

  val prelievoAggregato: String = "prelievo_aggregato"
  val anomalousDays: String = "anomalous_days"

  override val publicationType: String = INCOERENTIDETTAGLIO

  override val aggregatoColumnsConsumi: ListMap[String, String] = ListMap(
    AggConsumptionRequestRunnableSchema.pdr.toString -> SbgConsumiIncoerentiCOutputSchema.cod_pdr.toString,
    AggConsumptionRequestRunnableSchema.pivaDistr.toString -> SbgConsumiIncoerentiCOutputSchema.piva_distr.toString,
    AggConsumptionRequestRunnableSchema.pivaIt.toString -> SbgConsumiIncoerentiCOutputSchema.piva_it.toString,
    AggConsumptionRequestRunnableSchema.pivaUdd.toString -> SbgConsumiIncoerentiCOutputSchema.piva_udd.toString,
    AggConsumptionRequestRunnableSchema.pivaUdb.toString -> SbgConsumiIncoerentiCOutputSchema.piva_udb.toString,
    AggConsumptionRequestRunnableSchema.dtg.toString -> SbgConsumiIncoerentiCOutputSchema.dtg.toString,
    AggConsumptionRequestRunnableSchema.codRemi.toString -> SbgConsumiIncoerentiCOutputSchema.cod_remi.toString,
    AggConsumptionRequestRunnableSchema.ca.toString -> SbgConsumiIncoerentiCOutputSchema.prel_annuo_prev.toString,
    AggConsumptionRequestRunnableSchema.idRegClim.toString -> SbgConsumiIncoerentiCOutputSchema.id_reg_clim.toString,
    AggConsumptionRequestRunnableSchema.codProfStd.toString -> SbgConsumiIncoerentiCOutputSchema.cod_prof_prel_std.toString,
    AggConsumptionRequestRunnableSchema.treatment.toString -> SbgConsumiIncoerentiCOutputSchema.trattamento.toString,
    AggConsumptionRequestRunnableSchema.tipoCliente.toString -> SbgConsumiIncoerentiCOutputSchema.tipo_cliente.toString,
    AggConsumptionRequestRunnableSchema.unitMisPrel.toString -> SbgConsumiIncoerentiCOutputSchema.un_mis_prel.toString,
    AggConsumptionRequestRunnableSchema.classeMisuratore.toString -> SbgConsumiIncoerentiCOutputSchema.classe_gruppo_mis.toString,
    prelievoAggregato -> SbgConsumiIncoerentiCOutputSchema.prelievo_aggregato.toString,
    anomalousDays -> SbgConsumiIncoerentiCOutputSchema.giorno_sterilizzato.toString
  )

  /**
   * Esegue i filtri e le operazioni al fine di individuare i PdR incoerenti C a partire dal dataframe dei consumi [[df]].
   * @param df dataframe dei consumi
   * @return [[df]] contenente soltanto i PdR incoerenti C
   */
  override def consumptionFilter(df: DataFrame): DataFrame = {
    getAnomalousPdrs(df)
  }

  override def getPdr(df: DataFrame): DataFrame = {
    val columnsForGroupBy = (aggregatoColumnsConsumi.keySet.toSeq.union(keyFiledsPreRenamed.values.toSeq)).distinct.map(col)
    val windowForAnomalies = Window.partitionBy(columnsForGroupBy.diff(List(col(prelievoAggregato), col(anomalousDays))): _*)

    var aggDF = consumptionFilter(df)
      .withColumn(prelievoAggregato, round(sum(col(AggConsumptionRequestRunnableSchema.value)).over(windowForAnomalies)).cast(IntegerType))
      .withColumn(anomalousDays, sort_array(collect_list(when(col(AggConsumptionRequestRunnableSchema.isDayAnomalous), dayofmonth(col(AggConsumptionRequestRunnableSchema.date)))).over(windowForAnomalies)))
      .withColumn(anomalousDays, array_join(col(anomalousDays), ","))
      .withColumn(dayOfMonth, concat(lit(pivotPrefix), dayofmonth(col(AggConsumptionRequestRunnableSchema.date))))
      .groupBy(columnsForGroupBy: _*)
      .pivot(dayOfMonth, (1 to 31).map(pivotPrefix + _))
      .agg(round(sum(col(AggConsumptionRequestRunnableSchema.valueNotSterilized))).cast(IntegerType))
      .withColumn(SbgConsumiIncoerentiCOutputSchema.data, date_format(trunc(to_date(unix_timestamp(col(AggConsumptionRequestRunnableSchema.annoMese), "yyyyMM").cast(TimestampType)), "month"), "dd/MM/yyyy"))

    aggregatoColumnsConsumi.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumn(fileName, col(dailyName).cast(StringType))
    })

    aggDF.filter(keyFieldsConsumi.values.map(f => col(f).isNotNull).reduce(_ && _))
      .selectExpr(headerCsvConsumi.union(keyFieldsConsumi.values.toSeq).distinct: _*).na.fill("")
  }
}
