package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomanceOld
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionInputProcessSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, date_format, lit, min, round, sum, to_date, trunc, udf, unix_timestamp, when}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, TimestampType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait PdrDettaglioEsclusi extends RunnableAggregatorPerfomanceOld {

  val operationName = "INCOERENTI_EXC"
  val dataValColName = "data"
  val forceExclusion = "force"
  val mappingCausaleInternalErrorCodePriority = Map(
    "1" -> "5"
    , "2" -> "9"
    , "3" -> "7"
    , "4" -> "8"
    , "5" -> "2"
    , "6" -> "3"
    , "8" -> "6"
    , "9" -> "4"
    , forceExclusion -> "1"
  )

  val mappingCausalePriorityExternalErrorCode = Map(
    "5" -> "T4"
    , "9" -> "T8"
    , "7" -> "T6"
    , "8" -> "T7"
    , "2" -> "T1"
    , "3" -> "M2"
    , "6" -> "T5"
    , "4" -> "T3"
    , "1" -> "Tf"
  )

  val udfMappingCausaleInternalErrorCodePriority: UserDefinedFunction = udf((errorCode: String) => mappingCausaleInternalErrorCodePriority(errorCode))

  val udfMappingCausalePriorityExternalErrorCode: UserDefinedFunction = udf((errorCode: String) => mappingCausalePriorityExternalErrorCode(errorCode))

  def RdbPivaItFilter: Column = lit(true)

  override def getAggregato(df: DataFrame): DataFrame = {
    val anomalousPdrs = getExcludedPdrs(df)
    getAggregatoFromPdrs(anomalousPdrs)
  }

  def getAggregatoFromPdrs(df: DataFrame): DataFrame = {
    var aggDF = df
      .na.fill("Y", Seq(DailyConsumptionInputProcessSchema.treatment.toString))
      .filter(RdbPivaItFilter) //always true except for RdbEsclusi.scala
      .withColumn(dataValColName, date_format(trunc(to_date(unix_timestamp(col(DailyConsumptionInputProcessSchema.annoMese), "yyyyMM").cast(TimestampType)), "month").cast(StringType), "dd/MM/yyyy"))
      .withColumn(DailyConsumptionInputProcessSchema.errorCode, when(col(DailyConsumptionInputProcessSchema.forcedExclusion).cast(BooleanType), lit(forceExclusion)).otherwise(col(DailyConsumptionInputProcessSchema.errorCode)))
      .withColumn(DailyConsumptionInputProcessSchema.causale, udfMappingCausaleInternalErrorCodePriority(col(DailyConsumptionInputProcessSchema.errorCode)))


    aggDF = aggDF.groupBy((aggregatoColumns.keys.toList.diff(List(DailyConsumptionInputProcessSchema.causale.toString, DailyConsumptionInputProcessSchema.valuef3.toString)) ++ List(dataValColName)).map(col): _*)
      .agg(
        round(sum(col(DailyConsumptionInputProcessSchema.valuef3))).cast(IntegerType).as(DailyConsumptionInputProcessSchema.valuef3),
        min(col(DailyConsumptionInputProcessSchema.causale)).as(DailyConsumptionInputProcessSchema.causale))
      .withColumn(DailyConsumptionInputProcessSchema.causale, udfMappingCausalePriorityExternalErrorCode(col(DailyConsumptionInputProcessSchema.causale)))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumnRenamed(dailyName, fileName)
    })

    aggDF.selectExpr(List(dataValColName) ++ aggregatoColumns.values: _*)
  }

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_AGG_${operationName}_${annomese}_${timestamp}_${counterCsv}.csv"
  }
}
