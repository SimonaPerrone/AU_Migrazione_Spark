package it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi.ElencoFlussiDettaglioUnico
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.storage.StorageLevel

trait ElencoFlussiDettaglioUnicoSbg extends ElencoFlussiDettaglioUnico {
  override def specificFilterForIncoerentiGdm: Column = col(DailyConsumptionAggSchema.treatment).isin("G","M")

  def listOfCsvFields: List[String] = aggregatoColumns.keySet.toList.diff(List(DailyConsumptionAggSchema.value.toString, DailyConsumptionAggSchema.leftMeasureLocalFile.toString)).distinct

  // If something is modified here, it's important to modify it.eng.au.queryReport.query.dettaglioUnico.QueryElencoFlussiDettaglioUnico.getAggregato as well
  // They accomplish the same task, but getAggregato in query-report-sbg module has to retrieve additional fields
  override def getAggregato(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val windowSumValue = Window.partitionBy(listOfCsvFields.map(col): _*)

    val aggDf = df.na.fill("Y", Seq(DailyConsumptionAggSchema.treatment.toString))
      .filter(col(DailyConsumptionAggSchema.errorCode).isin(0, 10, 11, 12, 14) and
        not(col(DailyConsumptionAggSchema.forcedExclusion) <=> true) and
        (col(DailyConsumptionAggSchema.isValid) === true or (col(DailyConsumptionAggSchema.isValid) === false and !(col(DailyConsumptionAggSchema.idFormula) === 3))) and
        col(DailyConsumptionAggSchema.treatment).isNotNull and
        col(DailyConsumptionAggSchema.dtg).isNotNull and
        col(DailyConsumptionAggSchema.codRemi).isNotNull and
        col(DailyConsumptionAggSchema.codProfStd).isNotNull and
        col(DailyConsumptionAggSchema.tipoCliente).isNotNull and
        col(DailyConsumptionAggSchema.unitMisPrel).isNotNull and
        (col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull || col(DailyConsumptionAggSchema.rightMeasureLocalFile).isNotNull) and
        fileSpecificFilterExpression
      )

    // Anomalous GDM pdrs should be forced with valuef3 in anomalous days
    val forcedAggDf = aggDf
      .selectExpr(
        aggregatoColumns.keySet.toList.union(
          List(
            DailyConsumptionAggSchema.date.toString,
            DailyConsumptionAggSchema.rightMeasureLocalFile.toString,
            DailyConsumptionAggSchema.idFormula.toString)): _*
      )

    // We force valuef3 for excluded pdrs as well
    val excludedPdrs = getExcludedPdrs(df)
      .selectExpr(
        aggregatoColumns.keySet.toList.union(
          List(
            DailyConsumptionAggSchema.date.toString,
            DailyConsumptionAggSchema.rightMeasureLocalFile.toString,
            DailyConsumptionAggSchema.idFormula.toString)): _*
      )

    val filteredDF = forcedAggDf.union(excludedPdrs)
      .coalesce(forcedAggDf.rdd.getNumPartitions)
      .repartition(forcedAggDf.rdd.getNumPartitions)
      //The difference with AGG is that the value here is the total sum of the monthly consumption,
      // while in AGG it's the sum of the consumptions from measures taking part in the actual calculation (so it's computed after filtering the measures)
      .withColumn(DailyConsumptionAggSchema.value, round(sum(col(DailyConsumptionAggSchema.value)).over(windowSumValue), 3).cast(DoubleType))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    computeElencoFlussi(filteredDF, validateFlow)
  }

  override def computeAggregation(df: DataFrame): DataFrame = {
    df
      .select((listOfCsvFields :+ DailyConsumptionAggSchema.value.toString).map(col): _*)
      .distinct
  }
}
