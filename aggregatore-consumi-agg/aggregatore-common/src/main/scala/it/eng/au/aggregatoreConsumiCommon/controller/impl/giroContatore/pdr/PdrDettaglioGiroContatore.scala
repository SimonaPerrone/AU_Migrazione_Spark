package it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.traits.{RunnableAggregatorPerfomance, RunnableAggregatorPerfomanceOld}
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, GiroContatoreDettaglioSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, dayofmonth}
import org.apache.spark.sql.types.DateType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait PdrDettaglioGiroContatore extends RunnableAggregatorPerfomanceOld {
  override val operationName = "GIRO"
  override val csvFields: List[String] = List() //throw new Exception("not supported")
  override val header: String = "" //throw new Exception("not supported")

  override def getAggregato(df: DataFrame): DataFrame = {
    var finalDF = df
      .filter(col(DailyConsumptionAggSchema.idFormula) === 6)
      .filter(!col(DailyConsumptionAggSchema.errorCode).isin(13, 15))
      .withColumn(GiroContatoreDettaglioSchema.GIORN_GC, dayofmonth(col(DailyConsumptionAggSchema.date)))
      .withColumn(GiroContatoreDettaglioSchema.PRELIEVO_GIORNO_GC, col(DailyConsumptionAggSchema.value))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF
      .withColumn(GiroContatoreDettaglioSchema.data, col(GiroContatoreDettaglioSchema.data).cast(DateType))
      .withColumn(
        GiroContatoreDettaglioSchema.data,
        date_format(col(GiroContatoreDettaglioSchema.data), "dd/MM/yyyy")
      )
      .distinct
  }

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_${sessionName}_${operationName}_${annomese}_${timestamp}_${counterCsv}.csv"
  }

}
