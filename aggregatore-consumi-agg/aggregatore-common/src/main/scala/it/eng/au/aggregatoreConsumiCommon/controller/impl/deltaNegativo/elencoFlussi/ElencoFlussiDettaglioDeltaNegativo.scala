package it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.elencoFlussi
import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomanceOld
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DeltaNegativoElencoFlussiSchema, ValidatedFlowsAggSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, date_format, lag, lead, lit, rank, round, substring_index, when}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait ElencoFlussiDettaglioDeltaNegativo extends RunnableAggregatorPerfomanceOld {
  override val operationName = "DN"

  override def elencoFlussiSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull || col(DailyConsumptionAggSchema.rightMeasureLocalFile).isNotNull

  def getAggregato(df: DataFrame, validateFlow: DataFrame): DataFrame = {
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

    var finalDF =  flowsOfMonth.unionByName(measuresOut)
      .filter(col(ValidatedFlowsAggSchema.service)=!="IGMGPOST")
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
      .filter(col("flag_final")===1)
      .filter((col("flag_conv_plus") || col("flag_conv")) || (col("flag_mis_plus") || col("flag_mis")))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDF = finalDF.withColumnRenamed(dailyName, fileName)
    })

    finalDF
      .withColumnRenamed(DailyConsumptionAggSchema.pivaDistr, DeltaNegativoElencoFlussiSchema.piva_distr)
      .withColumnRenamed(DailyConsumptionAggSchema.pivaUdb, DeltaNegativoElencoFlussiSchema.piva_udb)
      .withColumnRenamed(DailyConsumptionAggSchema.pivaUdd, DeltaNegativoElencoFlussiSchema.piva_udd)
      .withColumn(
        DeltaNegativoElencoFlussiSchema.nomefile,
        concat(
          lit("/"),
          substring_index(col(DeltaNegativoElencoFlussiSchema.nomefile), "/", -3)
        )
      )
      .withColumn(DeltaNegativoElencoFlussiSchema.data_lettura, col(DeltaNegativoElencoFlussiSchema.data_lettura).cast(DateType))
      .withColumn(
        DeltaNegativoElencoFlussiSchema.data_lettura,
        date_format(col(DeltaNegativoElencoFlussiSchema.data_lettura), "dd/MM/yyyy")
      )
      .withColumn(DeltaNegativoElencoFlussiSchema.let_tot_prel, round(col(DeltaNegativoElencoFlussiSchema.let_tot_prel)).cast(IntegerType))
      .withColumn(DeltaNegativoElencoFlussiSchema.let_tot_conv, round(col(DeltaNegativoElencoFlussiSchema.let_tot_conv)).cast(IntegerType))
      .select(
        DeltaNegativoElencoFlussiSchema.pdr.toString,
        DeltaNegativoElencoFlussiSchema.nomefile.toString,
        DeltaNegativoElencoFlussiSchema.sessione.toString,
        DeltaNegativoElencoFlussiSchema.annomese.toString,
        DeltaNegativoElencoFlussiSchema.let_tot_prel.toString,
        DeltaNegativoElencoFlussiSchema.let_tot_conv.toString,
        DeltaNegativoElencoFlussiSchema.data_lettura.toString,
        DeltaNegativoElencoFlussiSchema.tipo_lettura.toString,
        DeltaNegativoElencoFlussiSchema.matr_mis.toString,
        DeltaNegativoElencoFlussiSchema.matr_conv.toString,
        DeltaNegativoElencoFlussiSchema.coeff_corr.toString,
        DeltaNegativoElencoFlussiSchema.piva_distr.toString,
        DeltaNegativoElencoFlussiSchema.piva_udb.toString,
        DeltaNegativoElencoFlussiSchema.piva_udd.toString
      )
      .distinct
  }

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_${sessionName}_${operationName}_${annomese}_FlussiDN_${timestamp}_${counterCsv}.csv"
  }

}
