package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomance
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IncoerentiDettaglioSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/** Si occupa della creazione del dataframe contenente i flussi da pubblicare nel file ElencoFlussi del processo Incoerenti GDM (o Dettaglio Incoerenti) */
trait ElencoFlussiDettaglioIncoerenti extends RunnableAggregatorPerfomance {
  override val operationName: String = "INCOERENTI_GDM"

  override def elencoFlussiSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull || col(DailyConsumptionAggSchema.rightMeasureLocalFile).isNotNull

  def getAggregato(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val anomalousPdrs = getAnomalousPdrs(df)
    getElencoFlussi(anomalousPdrs, validateFlow)
  }

  /**
   * Esegue le query finalizzate all'estrazione delle'elenco flussi da [[validateFlow]] per i PdR presenti in [[df]].
   * @param df tabella dei consumi filtrata
   * @param validateFlow tabella delle misure da cui estrarre i flussi
   * @return l'elenco dei flussi relativi all'insieme di PdR da pubblicare
   */
  // If something is modified here, it's important to modify it.eng.au.queryReport.query.dettaglioIncoerenti.QueryElencoFlussiIncoerentiDettaglio.getElencoFlussi as well
  // They accomplish the same task, but getElencoFlussi in query-report-sbg module has to retrieve additional fields
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
      .withColumnRenamed(DailyConsumptionAggSchema.pivaDistr, IncoerentiDettaglioSchema.piva_distr)
      .withColumnRenamed(DailyConsumptionAggSchema.pivaIt, IncoerentiDettaglioSchema.piva_it)
      .withColumnRenamed(DailyConsumptionAggSchema.pivaRdb, IncoerentiDettaglioSchema.piva_rdb)
      .withColumnRenamed(DailyConsumptionAggSchema.pivaUdb, IncoerentiDettaglioSchema.piva_udb)
      .withColumnRenamed(DailyConsumptionAggSchema.pivaUdd, IncoerentiDettaglioSchema.piva_udd)
      .withColumn(
        IncoerentiDettaglioSchema.nomefile,
        concat(
          lit("/"),
          substring_index(col(IncoerentiDettaglioSchema.nomefile), "/", -3)
        )
      )
      .withColumn(IncoerentiDettaglioSchema.data_lettura, col(IncoerentiDettaglioSchema.data_lettura).cast(DateType))
      .withColumn(
        IncoerentiDettaglioSchema.data_lettura,
        date_format(col(IncoerentiDettaglioSchema.data_lettura), "dd/MM/yyyy")
      )
      .withColumn(IncoerentiDettaglioSchema.let_tot_prel, round(col(IncoerentiDettaglioSchema.let_tot_prel)).cast(IntegerType))
      .withColumn(IncoerentiDettaglioSchema.let_tot_conv, round(col(IncoerentiDettaglioSchema.let_tot_conv)).cast(IntegerType))
      .select(
        IncoerentiDettaglioSchema.pdr.toString,
        IncoerentiDettaglioSchema.nomefile.toString,
        IncoerentiDettaglioSchema.sessione.toString,
        IncoerentiDettaglioSchema.annomese.toString,
        IncoerentiDettaglioSchema.let_tot_prel.toString,
        IncoerentiDettaglioSchema.let_tot_conv.toString,
        IncoerentiDettaglioSchema.data_lettura.toString,
        IncoerentiDettaglioSchema.tipo_lettura.toString,
        IncoerentiDettaglioSchema.matr_mis.toString,
        IncoerentiDettaglioSchema.matr_conv.toString,
        IncoerentiDettaglioSchema.coeff_cor.toString,
        IncoerentiDettaglioSchema.mot_ret_lett.toString,
        IncoerentiDettaglioSchema.cau_int_mis.toString,
        IncoerentiDettaglioSchema.cau_int_cor.toString,
        IncoerentiDettaglioSchema.piva_distr.toString,
        IncoerentiDettaglioSchema.piva_it.toString,
        IncoerentiDettaglioSchema.piva_rdb.toString,
        IncoerentiDettaglioSchema.piva_udb.toString,
        IncoerentiDettaglioSchema.piva_udd.toString
      )
      .distinct
  }

  override def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.head)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${publicationType}_${operationName}_${annoMese}_ElencoFlussi_${timestamp}_${counterCsv}.csv"
  }

  //Not used
  override def getAggregato(df: DataFrame): DataFrame = Environment.sqlContext.emptyDataFrame
}
