package it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.traits.{RunnableAggregatorPerfomance, RunnableAggregatorPerfomanceOld}
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DeltaNegativoDettaglioSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, dayofmonth, lit}
import org.apache.spark.sql.types.DateType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait PdrDettaglioDeltaNegativo extends RunnableAggregatorPerfomanceOld {
  override val operationName = "DN"
  override val csvFields: List[String] = List() //throw new Exception("not supported")
  override val header: String = "" //throw new Exception("not supported")

  override def getAggregato(df: DataFrame): DataFrame = {
    var finalDF = df
      .filter(col(DailyConsumptionAggSchema.errorCode) === 12)
      .withColumn(DeltaNegativoDettaglioSchema.GIORN_DN, dayofmonth(col(DailyConsumptionAggSchema.date)))
      .withColumn(DeltaNegativoDettaglioSchema.PRELIEVO_GIORNO_DN, lit(0.0))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF
      .withColumn(DeltaNegativoDettaglioSchema.data, col(DeltaNegativoDettaglioSchema.data).cast(DateType))
      .withColumn(
        DeltaNegativoDettaglioSchema.data,
        date_format(col(DeltaNegativoDettaglioSchema.data), "dd/MM/yyyy")
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
