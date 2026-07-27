package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomanceOld
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import org.apache.spark.sql.functions.{col, not, round, sum}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/** Si occupa della creazione del dataframe contenente i PdR da pubblicare nel file PdR del processo Dettaglio Unico */
trait PdrDettaglioUnico extends RunnableAggregatorPerfomanceOld {
  val operationName = "DETTAGLIO_UNICO"

  def getCsvFields(dfAggregato: DataFrame): List[String]

  //  override def tmpHdfsOutput: String = super.tmpHdfsOutput + "/PDR"
  def fileSpecificFilterExpression: Column
  override def excludedPdrsSpecificCondition: Column = col(DailyConsumptionAggSchema.treatment).isNotNull

  override val csvFields: List[String] = List() //throw new Exception("not supported")
  override val header: String = "" //throw new Exception("not supported")

  /**
   * Esegue una serie di operazioni per preparare la tabella dei consumi alla scrittura su CSV. In particolare,
   *  1. applica i filtri dell'operazione "Dettaglio Unico" al dataframe [[df]] (e.g. PdR validi, non esclusi e con trattamento diverso da NULL);
   *  1. tra i PdR ottenuti applica la sterilizzazione dei consumi nei giorni anomali per i PdR incoerenti GDM;
   *  1. dalla tabella dei consumi [[df]] estrae i PdR esclusi e ne sterilizza i consumi;
   *  1. effettua l'unione dei [[df]] ottenuti da 1. e 3.;
   *  1. effettua l'aggregazione sulle colonne da pubblicare [[aggregatoColumns]] e ne aggrega i consumi.
   * @param df dataframe dei consumi AGG/SBG
   *  @return dataframe pronto per la pubblicazione su CSV
   */
  override def getAggregato(df: DataFrame): DataFrame = {
    val aggDf = df.na.fill("Y", Seq(DailyConsumptionAggSchema.treatment.toString))
      .filter(col(DailyConsumptionAggSchema.errorCode).isin(0, 10, 11, 12, 14) and
        not(col(DailyConsumptionAggSchema.forcedExclusion) <=> true) and
        col(DailyConsumptionAggSchema.treatment).isNotNull and
        (col(DailyConsumptionAggSchema.isValid) === true or (col(DailyConsumptionAggSchema.isValid) === false and !(col(DailyConsumptionAggSchema.idFormula) === 3))) and
        col(DailyConsumptionAggSchema.dtg).isNotNull and
        col(DailyConsumptionAggSchema.codRemi).isNotNull and
        col(DailyConsumptionAggSchema.codProfStd).isNotNull and
        col(DailyConsumptionAggSchema.tipoCliente).isNotNull and
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
      .groupBy(aggregatoColumns.keySet.toList.diff(List(DailyConsumptionAggSchema.value.toString)).distinct.map(col): _*)
      .agg(round(sum(col(DailyConsumptionAggSchema.value)), 3).cast(DoubleType).as(DailyConsumptionAggSchema.value))


    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      finalDf = finalDf.withColumn(fileName, col(dailyName))
    })

    filteringAndSelect(finalDf)
  }

  // This method is overridden in QueryPdrDettaglioUnico
  def filteringAndSelect(df: DataFrame): DataFrame = {
    df
      .filter(col(keyPiva1).isNotNull && col(keyPiva2).isNotNull)
      .selectExpr(aggregatoColumns.values.toSeq: _*)
  }

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_${sessionName}_${annomese}_${timestamp}_${counterCsv}.csv"
  }
}
