package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait DtgTrait extends DettaglioGTrait {
  override val operationName = "DTG"
  override def excludedPdrsSpecificCondition: Column = col(DailyConsumptionAggSchema.treatment).isNotNull

  /**
   * Esegue una serie di operazioni per preparare la tabella dei consumi alla scrittura su CSV. In particolare,
   *  1. applica i filtri dell'operazione "Dtgettaglio PdR G" al dataframe [[df]] (e.g. PdR validi, non esclusi);
   *  1. tra i PdR ottenuti applica la sterilizzazione dei consumi nei giorni anomali per i PdR incoerenti GDM;
   *  1. dalla tabella dei consumi [[df]] estrae i PdR esclusi e ne sterilizza i consumi;
   *  1. effettua l'unione dei [[df]] ottenuti da 1. e 3.;
   *  1. effettua l'aggregazione sulle colonne da pubblicare [[aggregatoColumns]] e ne aggrega i consumi.
   * @param df dataframe dei consumi AGG/SBG
   *  @return dataframe pronto per la pubblicazione su CSV
   */
  override def getAggregato(df: DataFrame): DataFrame = {
    val orderedSelectList = List(DailyConsumptionAggSchema.annoMese.toString, dataValColName) ++ aggregatoColumns.values ++ (1 to 31).map(pivotPrefix + _)

    val aggDf = df
      .na.fill("Y", Seq(DailyConsumptionAggSchema.treatment.toString))
      .filter(col(DailyConsumptionAggSchema.errorCode).isin(0, 10, 11, 12, 14) and
        not(col(DailyConsumptionAggSchema.forcedExclusion) <=> true) and
        (col(DailyConsumptionAggSchema.isValid) === true or (col(DailyConsumptionAggSchema.isValid) === false and !(col(DailyConsumptionAggSchema.idFormula) === 3))) and
        col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
        col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
        col(DailyConsumptionAggSchema.pivaIt).isNotNull and
        col(DailyConsumptionAggSchema.dtg).isNotNull and
        col(DailyConsumptionAggSchema.codRemi).isNotNull and
        col(DailyConsumptionAggSchema.ca).isNotNull and
        col(DailyConsumptionAggSchema.codProfStd).isNotNull and
        col(DailyConsumptionAggSchema.tipoCliente).isNotNull and
        col(DailyConsumptionAggSchema.unitMisPrel).isNotNull and
        fileSpecificFilterExpression
      )

    // Anomalous GDM pdrs should be forced with valuef3 in anomalous days
    val forcedAggDf = aggDf
      .selectExpr(aggregatoColumns.keys.toList :+
        DailyConsumptionAggSchema.value.toString :+
        DailyConsumptionAggSchema.date.toString :+
        DailyConsumptionAggSchema.annoMese.toString: _*)

    // We force valuef3 for excluded pdrs as well
    val esclusiDf = getExcludedPdrs(df)
      .selectExpr(aggregatoColumns.keys.toList :+
        DailyConsumptionAggSchema.value.toString :+
        DailyConsumptionAggSchema.date.toString :+
        DailyConsumptionAggSchema.annoMese.toString: _*)

    var finalDf = forcedAggDf.union(esclusiDf)
      .coalesce(aggDf.rdd.getNumPartitions)
      .repartition(aggDf.rdd.getNumPartitions)
      .withColumn(dayOfMonth, concat(lit(pivotPrefix), dayofmonth(col(DailyConsumptionAggSchema.date))))
      .groupBy((aggregatoColumns.keySet.toList :+ DailyConsumptionAggSchema.annoMese.toString).distinct.map(col): _*)
      .pivot(dayOfMonth, (1 to 31).map(pivotPrefix + _))
      .agg(bround(sum(col(DailyConsumptionAggSchema.value)), 2).cast(DoubleType))
      .withColumn(dataValColName, date_format(trunc(to_date(unix_timestamp(col(DailyConsumptionAggSchema.annoMese), "yyyyMM").cast(TimestampType)), "month").cast(StringType), "dd/MM/yyyy"))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDf = finalDf.withColumnRenamed(dailyName, fileName)
    })

    finalDf.filter(keyFields.map(f => col(f).isNotNull).reduce(_ && _))
      .selectExpr(orderedSelectList: _*)
  }

  override def getZipOutputName(pivaFolder: String, publicationType: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${sessionName}_${operationName}_${year}_${timestamp}_1.zip"
    zipName
  }
}
