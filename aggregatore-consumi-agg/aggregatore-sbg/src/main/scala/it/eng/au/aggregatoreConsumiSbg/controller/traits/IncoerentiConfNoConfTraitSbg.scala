package it.eng.au.aggregatoreConsumiSbg.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}

/**
 * Si occupa della creazione dei file CONF/NOCONF per gli incoerenti A+B di SBG, che verranno poi inseriti nello stesso zip dell'usuale file di incoerenti A+B.
 */
trait IncoerentiConfNoConfTraitSbg extends IncoerentiTrait {
  def readCsvConfNoConf(csvPath: String): DataFrame = {
    Environment.spark.read
      .format("csv")
      .option("header", "false")
      .load(csvPath)
      .toDF("pdr")
      .withColumn("pdr", trim(col("pdr")))
  }

  def getAggregato(df: DataFrame, csvPath: String): DataFrame = {
    val listaPdr = readCsvConfNoConf(csvPath)

    val dayOfMonth = "dayOfMonth"
    val orderedSelectList = List(DailyConsumptionAggSchema.annoMese.toString, dataValColName) ++ aggregatoColumns.values ++ (1 to 31).map(pivotPrefix + _)

    var aggDF = df
      .na.fill("Y", Seq(DailyConsumptionAggSchema.treatment.toString))
      .filter(col(DailyConsumptionAggSchema.errorCode).isin(0, 10, 11, 12, 14) and
        not(col(DailyConsumptionAggSchema.forcedExclusion) <=> true) and
        (col(DailyConsumptionAggSchema.isValid) === true or (col(DailyConsumptionAggSchema.isValid) === false and !(col(DailyConsumptionAggSchema.idFormula) === 3))) and
        col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
        col(DailyConsumptionAggSchema.dtg).isNotNull and
        col(DailyConsumptionAggSchema.codRemi).isNotNull and
        col(DailyConsumptionAggSchema.codProfStd).isNotNull and
        col(DailyConsumptionAggSchema.tipoCliente).isNotNull and
        col(DailyConsumptionAggSchema.unitMisPrel).isNotNull and
        col(DailyConsumptionAggSchema.treatment).isin("G", "M") and
        fileSpecificFilterExpression
      )
      .join(listaPdr, df(DailyConsumptionAggSchema.pdr) === listaPdr("pdr"), "inner")
      .drop(listaPdr("pdr"))
      .withColumn(dayOfMonth, concat(lit(pivotPrefix), dayofmonth(col(DailyConsumptionAggSchema.date))))
      .groupBy((aggregatoColumns.keySet.toList :+ DailyConsumptionAggSchema.annoMese.toString).distinct.map(col): _*)
      .pivot(dayOfMonth, (1 to 31).map(pivotPrefix + _))
      .agg(round(sum(col(DailyConsumptionAggSchema.value))).cast(IntegerType))
      .withColumn(dataValColName, date_format(trunc(to_date(unix_timestamp(col(DailyConsumptionAggSchema.annoMese), "yyyyMM").cast(TimestampType)), "month"), "dd/MM/yyyy"))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumnRenamed(dailyName, fileName)
    })

    val aggregatoDF = aggDF.selectExpr(orderedSelectList: _*)
      .filter(keyFields.map(f => col(f).isNotNull).reduce(_ && _))

    convertColumnsToString(aggregatoDF).na.fill("")
  }
}
