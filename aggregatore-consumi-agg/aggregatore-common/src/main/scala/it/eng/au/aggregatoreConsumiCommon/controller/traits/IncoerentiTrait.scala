package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait IncoerentiTrait extends RunnableAggregatorPerfomance {
  val operationName = "INCOERENTI"

  val dataValColName = "data"
  val pivotPrefix = "prelievo_giorn_"

  /**
   * Esegue una serie di operazioni per preparare la tabella dei consumi alla scrittura su CSV. In particolare,
   *  1. applica i filtri dell'operazione "Incoerenti A+B" al dataframe [[df]] (e.g. PdR validi, non esclusi e con trattamento diverso da NULL);
   *  1. tra i PdR ottenuti applica la sterilizzazione dei consumi nei giorni anomali per i PdR incoerenti GDM;
   *  1. dalla tabella dei consumi [[df]] estrae i PdR esclusi e ne sterilizza i consumi;
   *  1. effettua l'unione dei [[df]] ottenuti da 1. e 3.;
   *  1. effettua l'aggregazione sulle colonne da pubblicare [[aggregatoColumns]] e ne aggrega i consumi.
   * @param df dataframe dei consumi AGG/SBG
   *  @return dataframe pronto per la pubblicazione su CSV
   */
  override def getAggregato(df: DataFrame): DataFrame = {
    val dayOfMonth = "dayOfMonth"
    val monthlyConsumption = "monthly_consumption"
    val isAnomalous = "is_anomalous"
    val hasAbAnomalyInMonth = "has_AB_anomaly_in_month"

    val windowByPdrInMonth = Window.partitionBy(col(DailyConsumptionAggSchema.pdr), col(DailyConsumptionAggSchema.annoMese))

    val orderedSelectList = List(DailyConsumptionAggSchema.annoMese.toString, dataValColName) ++ aggregatoColumns.values ++ (1 to 31).map(pivotPrefix + _)

    val anomalousConditionExpression = (// pdr con consumo giornaliero > del 30% del consumo mensile e ....
      (col(monthlyConsumption) * 0.3 < col(DailyConsumptionAggSchema.value)) and (
        (col(DailyConsumptionAggSchema.ca) >= lit(5000) and col(monthlyConsumption) >= lit(5000)) or
          (col(DailyConsumptionAggSchema.ca) < lit(5000) and col(monthlyConsumption) > lit(100000))
        )
      ) or (// consumo mensile maggiore della ca e ...
      col(monthlyConsumption) > col(DailyConsumptionAggSchema.ca) and
        (col(DailyConsumptionAggSchema.ca) >= lit(5000) or
          col(monthlyConsumption) > lit(100000)
          )
      )

    val aggDF = df
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

    var incoerentiDf = aggDF
      //we only take PdRs which are not GDM anomalous
      .where(not(col(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM)) or col(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM).isNull)
      .withColumn(monthlyConsumption, sum(DailyConsumptionAggSchema.value).over(windowByPdrInMonth))
      .withColumn(isAnomalous, when(anomalousConditionExpression, true).otherwise(false))
      //take consumptions for all the months if at least one day is anomalous
      .withColumn(hasAbAnomalyInMonth, max(col(isAnomalous)).over(Window.partitionBy(col(DailyConsumptionAggSchema.pdr), col(DailyConsumptionAggSchema.annoMese))))
      .where(col(hasAbAnomalyInMonth) === true)
      .withColumn(dayOfMonth, concat(lit(pivotPrefix), dayofmonth(col(DailyConsumptionAggSchema.date))))
      .groupBy((aggregatoColumns.keySet.toList :+ DailyConsumptionAggSchema.annoMese.toString).distinct.map(col): _*)
      .pivot(dayOfMonth, (1 to 31).map(pivotPrefix + _))
      .agg(round(sum(col(DailyConsumptionAggSchema.value))).cast(IntegerType))
      .withColumn(dataValColName, date_format(trunc(to_date(unix_timestamp(col(DailyConsumptionAggSchema.annoMese), "yyyyMM").cast(TimestampType)), "month"), "dd/MM/yyyy"))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      incoerentiDf = incoerentiDf.withColumnRenamed(dailyName, fileName)
    })

    incoerentiDf.selectExpr(orderedSelectList: _*)
  }

  override def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.head)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    //es (AGG|SBG)1_0123456789/2022/04/0123456789_(AGG|SBG)_INCOERENTI_202204_20220428105421_1.csv
    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${publicationType}_${operationName}_${annoMese}_${timestamp}_${counterCsv}.csv"
  }

  override def getZipOutputName(pivaFolder: String, publicationType: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_(AGG|SBG)_INCOERENTI_2022(04)_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_${publicationType}_${operationName}_${year}_${timestamp}_1.zip"
    zipName
  }
}
