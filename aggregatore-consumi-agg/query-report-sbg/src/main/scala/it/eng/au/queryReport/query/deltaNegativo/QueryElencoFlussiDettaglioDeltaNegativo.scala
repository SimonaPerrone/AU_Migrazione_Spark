package it.eng.au.queryReport.query.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.elencoFlussi.ElencoFlussiDettaglioDeltaNegativoSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.ElencoFlussiDeltaNegativoQuerySchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, lit, rank, when}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

import java.sql.Timestamp
import scala.collection.immutable.ListMap

object QueryElencoFlussiDettaglioDeltaNegativo extends QueryTrait with ElencoFlussiDettaglioDeltaNegativoSbg {
  override val queryName = "elencoFlussoDettaglioDeltaNegativo"
  override val tableName: String = Environment.getElencoFlussiDettaglioDeltaNegativoTableName

  override val outputSchema: SchemaEnum = ElencoFlussiDeltaNegativoQuerySchema
  override def hdfsOutputPath: String = Environment.getQueryElencoFlussiDettaglioDeltaNegativoHdfsPath

  override val aggregatoColumns: ListMap[String, String] = ListMap(
    ValidatedFlowsAggSchema.pdr.toString -> ElencoFlussiDeltaNegativoQuerySchema.pdr.toString,
    ValidatedFlowsAggSchema.localFile.toString -> ElencoFlussiDeltaNegativoQuerySchema.nome_file.toString,
    DailyConsumptionAggSchema.session.toString -> ElencoFlussiDeltaNegativoQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> ElencoFlussiDeltaNegativoQuerySchema.annomese.toString,
    ValidatedFlowsAggSchema.measure.toString -> ElencoFlussiDeltaNegativoQuerySchema.measure.toString,
    ValidatedFlowsAggSchema.converted.toString -> ElencoFlussiDeltaNegativoQuerySchema.converted.toString,
    ValidatedFlowsAggSchema.date.toString -> ElencoFlussiDeltaNegativoQuerySchema.data_lettura.toString,
    ValidatedFlowsAggSchema.readType.toString -> ElencoFlussiDeltaNegativoQuerySchema.read_type.toString,
    ValidatedFlowsAggSchema.serialNumberMis.toString -> ElencoFlussiDeltaNegativoQuerySchema.serial_number_mis.toString,
    ValidatedFlowsAggSchema.serialNumberConv.toString -> ElencoFlussiDeltaNegativoQuerySchema.serial_number_conv.toString,
    ValidatedFlowsAggSchema.nCoeffCor.toString -> ElencoFlussiDeltaNegativoQuerySchema.coeff_cor.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> ElencoFlussiDeltaNegativoQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> ElencoFlussiDeltaNegativoQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> ElencoFlussiDeltaNegativoQuerySchema.piva_udd.toString
  )

  override def elencoFlussiSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull || col(DailyConsumptionAggSchema.rightMeasureLocalFile).isNotNull

  def getQueryDF(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val dnPdrs = df
      .filter(col(DailyConsumptionAggSchema.errorCode) === 12)
    getElencoFlussi(dnPdrs, validateFlow)
  }

  /**
   * Esegue le query finalizzate all'estrazione delle'elenco flussi da [[validateFlow]] per i PdR presenti in [[df]].
   *
   * @param df           tabella dei consumi filtrata
   * @param validateFlow tabella delle misure da cui estrarre i flussi
   * @return l'elenco dei flussi relativi all'insieme di PdR da pubblicare
   */

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

    val windowSpec = Window
      .partitionBy(ValidatedFlowsAggSchema.pdr, DailyConsumptionAggSchema.session, DailyConsumptionAggSchema.annoMese) // partizione per gruppo logico
      .orderBy(ValidatedFlowsAggSchema.date)

    var finalDF = flowsOfMonth.unionByName(measuresOut)
      .filter(col(ValidatedFlowsAggSchema.service) =!= "IGMGPOST")
      .distinct
      .withColumn("prev_converted", lag(ValidatedFlowsAggSchema.converted, 1).over(windowSpec))
      .withColumn("prev_measure", lag(ValidatedFlowsAggSchema.measure, 1).over(windowSpec))
      .withColumn("diff_converted", col(ValidatedFlowsAggSchema.converted) - col("prev_converted"))
      .withColumn("diff_measure", col(ValidatedFlowsAggSchema.measure) - col("prev_measure"))
      .withColumn("flag", when(col("diff_converted") < 0 || col("diff_measure") < 0, lit(1)).otherwise(lit(0)))
      .withColumn("next_flag", lead("flag", 1).over(windowSpec))
      .withColumn("flag_final",
        when(col("flag") === 1 || col("next_flag") === 1, lit(1)).otherwise(lit(0))
      )
      .withColumn("prev_serialnumberconv", lag(ValidatedFlowsAggSchema.serialNumberConv, 1).over(windowSpec))
      .withColumn("prev_serialnumbermis", lag(ValidatedFlowsAggSchema.serialNumberMis, 1).over(windowSpec))
      .withColumn("flag_conv", when(
        col(ValidatedFlowsAggSchema.serialNumberConv).isNotNull &&
          col("prev_serialnumberconv").isNotNull &&
          (col(ValidatedFlowsAggSchema.serialNumberConv) === col("prev_serialnumberconv")) &&
          (col("diff_converted") < 0)
        , lit(true)
      ).otherwise(lit(false)))
      .withColumn("flag_mis", when(
        col(ValidatedFlowsAggSchema.serialNumberMis).isNotNull &&
          col("prev_serialnumbermis").isNotNull &&
          (col(ValidatedFlowsAggSchema.serialNumberMis) === col("prev_serialnumbermis")) &&
          (col("diff_measure") < 0)
        , lit(true)
      ).otherwise(lit(false)))
      .withColumn("flag_conv_plus", lead("flag_conv", 1).over(windowSpec))
      .withColumn("flag_mis_plus", lead("flag_mis", 1).over(windowSpec))
      .filter(col("flag_final") === 1)
      .filter((col("flag_conv_plus") || col("flag_conv")) || (col("flag_mis_plus") || col("flag_mis")))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF
      .withColumn(ElencoFlussiDeltaNegativoQuerySchema.coeff_cor, col(ElencoFlussiDeltaNegativoQuerySchema.coeff_cor).cast(DoubleType))
      .select(
        ElencoFlussiDeltaNegativoQuerySchema.pdr.toString,
        ElencoFlussiDeltaNegativoQuerySchema.nome_file.toString,
        ElencoFlussiDeltaNegativoQuerySchema.sessione.toString,
        ElencoFlussiDeltaNegativoQuerySchema.annomese.toString,
        ElencoFlussiDeltaNegativoQuerySchema.measure.toString,
        ElencoFlussiDeltaNegativoQuerySchema.converted.toString,
        ElencoFlussiDeltaNegativoQuerySchema.data_lettura.toString,
        ElencoFlussiDeltaNegativoQuerySchema.read_type.toString,
        ElencoFlussiDeltaNegativoQuerySchema.serial_number_mis.toString,
        ElencoFlussiDeltaNegativoQuerySchema.serial_number_conv.toString,
        ElencoFlussiDeltaNegativoQuerySchema.coeff_cor.toString,
        ElencoFlussiDeltaNegativoQuerySchema.piva_distr.toString,
        ElencoFlussiDeltaNegativoQuerySchema.piva_udb.toString,
        ElencoFlussiDeltaNegativoQuerySchema.piva_udd.toString
      )
      .distinct
  }

  override def writeOnHive(df: DataFrame): Unit = {
    df
      .withColumn("dailyconsumption_executionid", lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn("executionid", lit(Timestamp.valueOf(Environment.getDateRun).getTime))
      .selectExpr(outputSchema.getValues:_*)
      .write
      .partitionBy("executionid").mode(SaveMode.Append).parquet(hdfsOutputPath)

    if (!Environment.isLocalMode) Environment.spark.sql(s"MSCK REPAIR TABLE $tableName")
  }

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df

  override val csvFields: List[String] = List()
  override val baseNumber: String = ""
  override val mainPiva: String = ""
  override val keyPiva1: String = ""
  override val keyPiva2: String = ""
  override val header: String = ""
}
