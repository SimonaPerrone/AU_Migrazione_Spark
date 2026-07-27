package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.controller.traits.{RunnableAggregatorPerfomance, RunnableAggregatorPerfomanceOld}
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, EsclusiElencoFlussiOutputSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat, date_format, lit, rank, round, substring_index}
import org.apache.spark.sql.types.{DateType, IntegerType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait ElencoFlussiDettaglioEsclusi extends RunnableAggregatorPerfomanceOld {

  override val operationName = "INCOERENTI_EXC"
  override def elencoFlussiSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull || col(DailyConsumptionAggSchema.rightMeasureLocalFile).isNotNull

  def getAggregato(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val excludedPdrs = getExcludedPdrs(df)
    getElencoFlussi(excludedPdrs, validateFlow)
  }

  def getElencoFlussi(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val leftMeasures = validateFlow
      .join(
        df.select(
          DailyConsumptionAggSchema.pdr,
          DailyConsumptionAggSchema.leftMeasureLocalFile
        ).withColumnRenamed(DailyConsumptionAggSchema.leftMeasureLocalFile, ValidatedFlowsAggSchema.localFile),
        Seq(ValidatedFlowsAggSchema.pdr.toString, ValidatedFlowsAggSchema.localFile.toString),
        "inner"
      )

    val rightMeasures = validateFlow
      .join(
        df.select(
          DailyConsumptionAggSchema.pdr,
          DailyConsumptionAggSchema.rightMeasureLocalFile
        ).withColumnRenamed(DailyConsumptionAggSchema.rightMeasureLocalFile, ValidatedFlowsAggSchema.localFile),
        Seq(ValidatedFlowsAggSchema.pdr.toString, ValidatedFlowsAggSchema.localFile.toString),
        "inner"
      )

    val validatedMeasures = leftMeasures
      .unionByName(rightMeasures)
      .distinct

    val dailyInfosMonth = df
      .select(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.date,
        DailyConsumptionAggSchema.pivaDistr,
        DailyConsumptionAggSchema.pivaIt,
        DailyConsumptionAggSchema.pivaRdb,
        DailyConsumptionAggSchema.pivaUdb,
        DailyConsumptionAggSchema.pivaUdd,
        DailyConsumptionAggSchema.annoMese,
        DailyConsumptionAggSchema.session
      )

    val enrichedFlows = validatedMeasures
      .join(dailyInfosMonth, Seq(ValidatedFlowsAggSchema.pdr.toString, ValidatedFlowsAggSchema.date.toString), "left")

    val flowsOfMonth = enrichedFlows.filter(col(DailyConsumptionAggSchema.session).isNotNull)
    val flowsOutOfMonth = enrichedFlows.filter(col(DailyConsumptionAggSchema.session).isNull).selectExpr(validateFlow.columns: _*)

    val windowSpecLeft = Window
      .partitionBy(
        col(ValidatedFlowsAggSchema.pdr.toString),
        col(ValidatedFlowsAggSchema.date.toString),
        col(ValidatedFlowsAggSchema.service.toString)
      )
      .orderBy(col("data_rif").asc)

    val windowSpecRight = Window
      .partitionBy(
        col(ValidatedFlowsAggSchema.pdr.toString),
        col(ValidatedFlowsAggSchema.date.toString),
        col(ValidatedFlowsAggSchema.service.toString)
      )
      .orderBy(col("data_rif").desc)

    val leftMeasuresOut = flowsOutOfMonth
      .join(
        df.select(
          DailyConsumptionAggSchema.pdr,
          DailyConsumptionAggSchema.date,
          DailyConsumptionAggSchema.leftMeasureLocalFile,
          DailyConsumptionAggSchema.pivaDistr,
          DailyConsumptionAggSchema.pivaIt,
          DailyConsumptionAggSchema.pivaRdb,
          DailyConsumptionAggSchema.pivaUdb,
          DailyConsumptionAggSchema.pivaUdd,
          DailyConsumptionAggSchema.annoMese,
          DailyConsumptionAggSchema.session
        )
          .withColumnRenamed(DailyConsumptionAggSchema.leftMeasureLocalFile, ValidatedFlowsAggSchema.localFile)
          .withColumnRenamed(DailyConsumptionAggSchema.date, "data_rif"),
        Seq(ValidatedFlowsAggSchema.pdr.toString, ValidatedFlowsAggSchema.localFile.toString),
        "inner"
      ).withColumn("rank", rank().over(windowSpecLeft))
      .filter(col("rank") === 1)
      .drop("rank", "data_rif")

    val rightMeasuresOut = flowsOutOfMonth
      .join(
        df.select(
          DailyConsumptionAggSchema.pdr,
          DailyConsumptionAggSchema.date,
          DailyConsumptionAggSchema.rightMeasureLocalFile,
          DailyConsumptionAggSchema.pivaDistr,
          DailyConsumptionAggSchema.pivaIt,
          DailyConsumptionAggSchema.pivaRdb,
          DailyConsumptionAggSchema.pivaUdb,
          DailyConsumptionAggSchema.pivaUdd,
          DailyConsumptionAggSchema.annoMese,
          DailyConsumptionAggSchema.session
        ).withColumnRenamed(DailyConsumptionAggSchema.rightMeasureLocalFile, ValidatedFlowsAggSchema.localFile)
          .withColumnRenamed(DailyConsumptionAggSchema.date, "data_rif"),
        Seq(ValidatedFlowsAggSchema.pdr.toString, ValidatedFlowsAggSchema.localFile.toString),
        "inner"
      ).withColumn("rank", rank().over(windowSpecRight))
      .filter(col("rank") === 1)
      .drop("rank", "data_rif")

    val measuresOut = leftMeasuresOut.unionByName(rightMeasuresOut).distinct

    var finalDF = flowsOfMonth.unionByName(measuresOut).distinct

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF
      .withColumnRenamed(DailyConsumptionAggSchema.pivaDistr, "piva_distr")
      .withColumnRenamed(DailyConsumptionAggSchema.pivaIt, "piva_it")
      .withColumnRenamed(DailyConsumptionAggSchema.pivaRdb, "piva_rdb")
      .withColumnRenamed(DailyConsumptionAggSchema.pivaUdb, "piva_udb")
      .withColumnRenamed(DailyConsumptionAggSchema.pivaUdd, "piva_udd")
      .withColumn(
        EsclusiElencoFlussiOutputSchema.nomefile,
        concat(
          lit("/"),
          substring_index(col(EsclusiElencoFlussiOutputSchema.nomefile), "/", -3)
        )
      )
      .withColumn(EsclusiElencoFlussiOutputSchema.data_lettura, col(EsclusiElencoFlussiOutputSchema.data_lettura).cast(DateType))
      .withColumn(
        EsclusiElencoFlussiOutputSchema.data_lettura,
        date_format(col(EsclusiElencoFlussiOutputSchema.data_lettura), "dd/MM/yyyy")
      )
      .withColumn(EsclusiElencoFlussiOutputSchema.let_tot_prel, round(col(EsclusiElencoFlussiOutputSchema.let_tot_prel)).cast(IntegerType))
      .withColumn(EsclusiElencoFlussiOutputSchema.let_tot_conv, round(col(EsclusiElencoFlussiOutputSchema.let_tot_conv)).cast(IntegerType))
      .select(
        EsclusiElencoFlussiOutputSchema.pdr.toString,
        EsclusiElencoFlussiOutputSchema.nomefile.toString,
        EsclusiElencoFlussiOutputSchema.sessione.toString,
        EsclusiElencoFlussiOutputSchema.annoMese.toString,
        EsclusiElencoFlussiOutputSchema.let_tot_prel.toString,
        EsclusiElencoFlussiOutputSchema.let_tot_conv.toString,
        EsclusiElencoFlussiOutputSchema.data_lettura.toString,
        EsclusiElencoFlussiOutputSchema.tipo_lettura.toString,
        EsclusiElencoFlussiOutputSchema.matr_mis.toString,
        EsclusiElencoFlussiOutputSchema.matr_conv.toString,
        EsclusiElencoFlussiOutputSchema.coeff_cor.toString,
        EsclusiElencoFlussiOutputSchema.mot_ret_lett.toString,
        EsclusiElencoFlussiOutputSchema.cau_int_mis.toString,
        EsclusiElencoFlussiOutputSchema.cau_int_cor.toString,
        "piva_distr",
        "piva_it",
        "piva_rdb",
        "piva_udb",
        "piva_udd"
      )
      .distinct
  }

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_AGG_${operationName}_${annomese}_ElencoFlussi_${timestamp}_${counterCsv}.csv"
  }

  //Not used
  override def getAggregato(df: DataFrame): DataFrame = Environment.sqlContext.emptyDataFrame
}
