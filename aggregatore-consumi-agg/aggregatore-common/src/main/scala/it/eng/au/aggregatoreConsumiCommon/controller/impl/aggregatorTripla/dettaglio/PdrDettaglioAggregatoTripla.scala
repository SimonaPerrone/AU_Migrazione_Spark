package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.dettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomance
import it.eng.au.aggregatoreConsumiCommon.schema.{AggregatoTriplaSchema, DailyConsumptionInputProcessSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, date_format, date_trunc, round, sum}
import org.apache.spark.sql.types.StringType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait PdrDettaglioAggregatoTripla extends RunnableAggregatorPerfomance {
  override val operationName = "DISTR_IT_REMI"
  override val csvFields: List[String] = List() //throw new Exception("not supported")

  override def getAggregato(df: DataFrame): DataFrame = {
    val groupByCols = Seq(
      col(DailyConsumptionInputProcessSchema.date.toString),
      col(DailyConsumptionInputProcessSchema.pivaDistr.toString),
      col(DailyConsumptionInputProcessSchema.pivaIt.toString),
      col(DailyConsumptionInputProcessSchema.pivaRdb.toString),
      col(DailyConsumptionInputProcessSchema.codRemi.toString),
      col(DailyConsumptionInputProcessSchema.annoMese.toString)
    )
    var result = df
      .filter(
        col(DailyConsumptionInputProcessSchema.errorCode).isin(0, 1, 2, 5, 6, 9, 10, 11, 12)
        and col(DailyConsumptionInputProcessSchema.pivaUdd).isNotNull
        and col(DailyConsumptionInputProcessSchema.dtg).isNotNull
        and (col(DailyConsumptionInputProcessSchema.isValid) === true
          or (col(DailyConsumptionInputProcessSchema.isValid) === false and col(DailyConsumptionInputProcessSchema.idFormula) =!= 3))
        and col(DailyConsumptionInputProcessSchema.codRemi).isNotNull
        and col(DailyConsumptionInputProcessSchema.codProfStd).isNotNull
        and col(DailyConsumptionInputProcessSchema.tipoCliente).isNotNull
        and col(DailyConsumptionInputProcessSchema.unitMisPrel).isNotNull
        and col(DailyConsumptionInputProcessSchema.pivaDistr).isNotNull
      )
      .withColumn(DailyConsumptionInputProcessSchema.date, date_format(date_trunc("month", col(DailyConsumptionInputProcessSchema.date)), "dd/MM/yyyy"))
      .groupBy(groupByCols:_*)
      .agg(sum(DailyConsumptionInputProcessSchema.value).alias(AggregatoTriplaSchema.consumo))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      result = result.withColumnRenamed(dailyName, fileName)
    })

    result
      .withColumn(AggregatoTriplaSchema.consumo, round(col(AggregatoTriplaSchema.consumo), 3))
      .selectExpr(AggregatoTriplaSchema.getValues:_*)
  }

  override def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.head)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${annoMese}_${operationName}_${timestamp}_${counterCsv}.csv"
  }
}
