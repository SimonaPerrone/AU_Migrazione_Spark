package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomance
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, TimestampType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/** Si occupa della creazione del dataframe contenente i PdR da pubblicare nel file PdR del processo Incoerenti GDM (o Dettaglio Incoerenti) */
trait PdrDettaglioIncoerenti extends RunnableAggregatorPerfomance {
  override val operationName: String = "INCOERENTI_GDM"

  val anomalousDays = "anomalous_days"
  val prelievoAggregato = "prelievo_aggregato"
  val prelievoNonSterilizzato = "prelievo_non_sterilizzato"
  val dataValColName = "data"
  val pivotPrefix = "prelievo_non_sterilizzato_giorn_"

  override def getAggregato(df: DataFrame): DataFrame = {
    val anomalousPdrs = getAnomalousPdrs(df)
    getAggregatoFromPdrs(anomalousPdrs)
  }

  /**
   * Esegue una serie di operazioni per preparare la tabella dei consumi alla scrittura su CSV.
   * In particolare,
   *  1. calcola il campo `prelievo_aggregato`, ovvero la somma dei prelievi sterilizzati per il mese considerato;
   *  1. calcola il campo `giorno_sterilizzato`, ovvero la lista dei giorni in cui è stato trovato un consumo incoerente;
   *  1. raggruppa i record tramite le colonne da pubblicare [[aggregatoColumns]] e ne aggrega i consumi non sterilizzati.
   * @param df tabella dei consumi
   * @return tabella dei consumi filtrata, e con i campi necessari alla scrittura dei consumi su file CSV
   */
  def getAggregatoFromPdrs(df: DataFrame): DataFrame = {
    val dayOfMonth = "dayOfMonth"

    val columnsForGroupBy = (aggregatoColumns.keySet.toList :+ DailyConsumptionAggSchema.annoMese.toString).distinct.map(col)
    val windowForAnomalies = Window.partitionBy(columnsForGroupBy.diff(List(col(prelievoAggregato), col(anomalousDays))): _*)

    val orderedSelectList = List(DailyConsumptionAggSchema.annoMese.toString, dataValColName) ++ aggregatoColumns.values ++ (1 to 31).map(pivotPrefix + _)

    var aggDF = df
      .withColumn(prelievoNonSterilizzato, when(col(DailyConsumptionInputProcessSchema.isDayAnomalous), col(DailyConsumptionInputProcessSchema.valueNotSterilizedI)).otherwise(col(DailyConsumptionInputProcessSchema.value)))//col(DailyConsumptionAggSchema.valuef3)).otherwise(col(DailyConsumptionAggSchema.value)))
      .withColumn(prelievoAggregato, round(sum(col(DailyConsumptionInputProcessSchema.value)).over(windowForAnomalies)).cast(IntegerType))
      .withColumn(anomalousDays, sort_array(collect_list(when(col(DailyConsumptionInputProcessSchema.isDayAnomalous), dayofmonth(col(DailyConsumptionAggSchema.date)))).over(windowForAnomalies)))
      .withColumn(anomalousDays, array_join(col(anomalousDays), ","))
      .withColumn(dayOfMonth, concat(lit(pivotPrefix), dayofmonth(col(DailyConsumptionInputProcessSchema.date))))
      .groupBy(columnsForGroupBy: _*)
      .pivot(dayOfMonth, (1 to 31).map(pivotPrefix + _))
      .agg(round(sum(col(prelievoNonSterilizzato))).cast(LongType))
      .withColumn(dataValColName, date_format(trunc(to_date(unix_timestamp(col(DailyConsumptionInputProcessSchema.annoMese), "yyyyMM").cast(TimestampType)), "month"), "dd/MM/yyyy"))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumnRenamed(dailyName, fileName)
    })

    aggDF.selectExpr(orderedSelectList: _*)
  }

  override def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.head)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${publicationType}_${operationName}_${annoMese}_${timestamp}_${counterCsv}.csv"
  }
}
