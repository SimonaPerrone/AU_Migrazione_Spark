package it.eng.au.queryReport.query.esclusi

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.elencoFlussi.ElencoFlussiDettaglioEsclusiSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.{EsclusiElencoFlussiQuerySchema, ValidatedFlowsSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, rank}
import org.apache.spark.sql.types.{DoubleType, LongType}

import java.sql.Timestamp
import scala.collection.immutable.ListMap

object QueryElencoFlussiDettaglioEsclusi extends QueryTrait with ElencoFlussiDettaglioEsclusiSbg {
  override val queryName = "elencoFlussoDettaglioEsclusi"
  override val tableName: String = Environment.getElencoFlussiDettaglioEsclusiTableName

  val aggregatoColumns: ListMap[String, String] = ListMap(
    ValidatedFlowsAggSchema.pdr.toString -> EsclusiElencoFlussiQuerySchema.pdr.toString,
    ValidatedFlowsAggSchema.date.toString -> EsclusiElencoFlussiQuerySchema.data_lettura.toString,
    ValidatedFlowsAggSchema.treatment.toString -> EsclusiElencoFlussiQuerySchema.trattamento.toString,
    ValidatedFlowsAggSchema.localFile.toString -> EsclusiElencoFlussiQuerySchema.nome_file.toString,
    DailyConsumptionAggSchema.session.toString -> EsclusiElencoFlussiQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> EsclusiElencoFlussiQuerySchema.annomese.toString,
    ValidatedFlowsAggSchema.nCoeffCor.toString -> EsclusiElencoFlussiQuerySchema.coeff_cor.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> EsclusiElencoFlussiQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> EsclusiElencoFlussiQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> EsclusiElencoFlussiQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> EsclusiElencoFlussiQuerySchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> EsclusiElencoFlussiQuerySchema.piva_rdb.toString,
    ValidatedFlowsAggSchema.service.toString -> EsclusiElencoFlussiQuerySchema.service.toString,
    ValidatedFlowsSchema.readtype.toString -> EsclusiElencoFlussiQuerySchema.read_type.toString,
    ValidatedFlowsSchema.motivation.toString -> EsclusiElencoFlussiQuerySchema.motivation.toString,
    ValidatedFlowsSchema.serialnumbermis.toString -> EsclusiElencoFlussiQuerySchema.serial_number_mis.toString,
    ValidatedFlowsSchema.serialnumberconv.toString -> EsclusiElencoFlussiQuerySchema.serial_number_conv.toString,
    ValidatedFlowsSchema.measure.toString -> EsclusiElencoFlussiQuerySchema.measure.toString,
    ValidatedFlowsSchema.converted.toString -> EsclusiElencoFlussiQuerySchema.converted.toString,
    ValidatedFlowsSchema.cauintmis.toString -> EsclusiElencoFlussiQuerySchema.cau_int_mis.toString,
    ValidatedFlowsSchema.cauintcorr.toString -> EsclusiElencoFlussiQuerySchema.cau_int_corr.toString
  )

  override val outputSchema: SchemaEnum = EsclusiElencoFlussiQuerySchema
  override def hdfsOutputPath: String = Environment.getQueryElencoFlussiDettaglioEsclusiHdfsPath

  def getQueryDF(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val excludedPdrs = getExcludedPdrs(df)
    getElencoFlussi(excludedPdrs, validateFlow)
      .withColumn(EsclusiElencoFlussiQuerySchema.dailyconsumption_executionid, lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn(EsclusiElencoFlussiQuerySchema.executionid, lit(Timestamp.valueOf(Environment.getDateRun).getTime))
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
      .withColumn(EsclusiElencoFlussiQuerySchema.coeff_cor, col(EsclusiElencoFlussiQuerySchema.coeff_cor).cast(DoubleType))
      .select(
        EsclusiElencoFlussiQuerySchema.pdr.toString,
        EsclusiElencoFlussiQuerySchema.nome_file.toString,
        EsclusiElencoFlussiQuerySchema.service.toString,
        EsclusiElencoFlussiQuerySchema.sessione.toString,
        EsclusiElencoFlussiQuerySchema.annomese.toString,
        EsclusiElencoFlussiQuerySchema.measure.toString,
        EsclusiElencoFlussiQuerySchema.converted.toString,
        EsclusiElencoFlussiQuerySchema.data_lettura.toString,
        EsclusiElencoFlussiQuerySchema.read_type.toString,
        EsclusiElencoFlussiQuerySchema.serial_number_mis.toString,
        EsclusiElencoFlussiQuerySchema.serial_number_conv.toString,
        EsclusiElencoFlussiQuerySchema.coeff_cor.toString,
        EsclusiElencoFlussiQuerySchema.motivation.toString,
        EsclusiElencoFlussiQuerySchema.cau_int_mis.toString,
        EsclusiElencoFlussiQuerySchema.cau_int_corr.toString,
        EsclusiElencoFlussiQuerySchema.trattamento.toString,
        EsclusiElencoFlussiQuerySchema.piva_distr.toString,
        EsclusiElencoFlussiQuerySchema.piva_it.toString,
        EsclusiElencoFlussiQuerySchema.piva_rdb.toString,
        EsclusiElencoFlussiQuerySchema.piva_udb.toString,
        EsclusiElencoFlussiQuerySchema.piva_udd.toString
      )
      .distinct
  }

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df

  override val keyPiva1: String = ""
  override val keyPiva2: String = ""
  override val header: String = ""
  override val csvFields: List[String] = List()
  override val baseNumber: String = ""
  override val mainPiva: String = ""
}
