package it.eng.au.queryReport.query.dettaglioIncoerenti

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.elencoFlussi.ElencoFlussiDettaglioIncoerentiSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.{ElencoFlussiDIQuerySchema, ValidatedFlowsSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{Column, DataFrame}

import java.sql.Timestamp
import scala.collection.immutable.ListMap

/** Esegue la query della pubblicazione definita in [[ElencoFlussiDettaglioIncoerentiSbg]]. */
object QueryElencoFlussiDettaglioIncoerenti extends QueryTrait with ElencoFlussiDettaglioIncoerentiSbg {
  override val queryName = "elencoFlussoDettaglioIncoerenti"
  override val tableName: String = Environment.getElencoFlussiDettaglioIncoerentiTableName

  val aggregatoColumns: ListMap[String, String] = ListMap(
    ValidatedFlowsSchema.pdr.toString -> ElencoFlussiDIQuerySchema.pdr.toString,
    ValidatedFlowsSchema.date.toString -> ElencoFlussiDIQuerySchema.data_lettura.toString,
    ValidatedFlowsSchema.treatment.toString -> ElencoFlussiDIQuerySchema.trattamento.toString,
    ValidatedFlowsSchema.localfile.toString -> ElencoFlussiDIQuerySchema.nome_file.toString,
    DailyConsumptionAggSchema.session.toString -> ElencoFlussiDIQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> ElencoFlussiDIQuerySchema.annomese.toString,
    ValidatedFlowsSchema.ncoeffcor.toString -> ElencoFlussiDIQuerySchema.coeff_cor.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> ElencoFlussiDIQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> ElencoFlussiDIQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> ElencoFlussiDIQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> ElencoFlussiDIQuerySchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> ElencoFlussiDIQuerySchema.piva_rdb.toString,
    ValidatedFlowsSchema.readtype.toString -> ElencoFlussiDIQuerySchema.read_type.toString,
    ValidatedFlowsSchema.motivation.toString -> ElencoFlussiDIQuerySchema.motivation.toString,
    ValidatedFlowsSchema.serialnumbermis.toString -> ElencoFlussiDIQuerySchema.serial_number_mis.toString,
    ValidatedFlowsSchema.serialnumberconv.toString -> ElencoFlussiDIQuerySchema.serial_number_conv.toString,
    ValidatedFlowsSchema.measure.toString -> ElencoFlussiDIQuerySchema.measure.toString,
    ValidatedFlowsSchema.converted.toString -> ElencoFlussiDIQuerySchema.converted.toString,
    ValidatedFlowsSchema.cauintmis.toString -> ElencoFlussiDIQuerySchema.cau_int_mis.toString,
    ValidatedFlowsSchema.cauintcorr.toString -> ElencoFlussiDIQuerySchema.cau_int_corr.toString
  )

  override val outputSchema: SchemaEnum = ElencoFlussiDIQuerySchema

  override def hdfsOutputPath: String = Environment.getQueryElencoFlussiDettaglioIncoerentiHdfsPath

  def getQueryDF(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val anomalousPdrsForElencoFlussi = getAnomalousPdrs(df)

    getElencoFlussi(anomalousPdrsForElencoFlussi, validateFlow)
      .withColumn(ElencoFlussiDIQuerySchema.dailyconsumption_executionid, lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn(ElencoFlussiDIQuerySchema.executionid, lit(Timestamp.valueOf(Environment.getDateRun).getTime))
      .selectExpr(outputSchema.getValues: _*)
  }

  override def getElencoFlussi(df: DataFrame, validateFlow: DataFrame): DataFrame = {
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
    val flowsOutOfMonth = enrichedFlows.filter(col(DailyConsumptionAggSchema.session).isNull).selectExpr(validateFlow.columns:_*)

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
      .filter(col("rank")===1)
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
      .filter(col("rank")===1)
      .drop("rank", "data_rif")

    val measuresOut = leftMeasuresOut.unionByName(rightMeasuresOut).distinct

    var finalDF = flowsOfMonth.unionByName(measuresOut).distinct

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF
      .withColumn(ElencoFlussiDIQuerySchema.coeff_cor, col(ElencoFlussiDIQuerySchema.coeff_cor).cast(DoubleType))
      .select(
      ElencoFlussiDIQuerySchema.pdr.toString,
      ElencoFlussiDIQuerySchema.nome_file.toString,
      ElencoFlussiDIQuerySchema.sessione.toString,
      ElencoFlussiDIQuerySchema.annomese.toString,
      ElencoFlussiDIQuerySchema.measure.toString,
      ElencoFlussiDIQuerySchema.converted.toString,
      ElencoFlussiDIQuerySchema.data_lettura.toString,
      ElencoFlussiDIQuerySchema.read_type.toString,
      ElencoFlussiDIQuerySchema.serial_number_mis.toString,
      ElencoFlussiDIQuerySchema.serial_number_conv.toString,
      ElencoFlussiDIQuerySchema.coeff_cor.toString,
      ElencoFlussiDIQuerySchema.motivation.toString,
      ElencoFlussiDIQuerySchema.cau_int_mis.toString,
      ElencoFlussiDIQuerySchema.cau_int_corr.toString,
      ElencoFlussiDIQuerySchema.trattamento.toString,
      ElencoFlussiDIQuerySchema.service.toString,
      ElencoFlussiDIQuerySchema.piva_distr.toString,
      ElencoFlussiDIQuerySchema.piva_it.toString,
      ElencoFlussiDIQuerySchema.piva_rdb.toString,
      ElencoFlussiDIQuerySchema.piva_udb.toString,
      ElencoFlussiDIQuerySchema.piva_udd.toString
    )
      .distinct
  }

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
    col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df

  override val keyFields: List[String] = List()
  override val csvFields: List[String] = List()
  override val baseNumber: String = ""
  override val mainPiva: String = ""
}