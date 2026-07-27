package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi

import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomanceOld
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, ValidatedFlowsAggSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/** Si occupa della creazione del dataframe contenente i flussi da pubblicare nel file ElencoFlussi del processo Dettaglio Unico */
trait ElencoFlussiDettaglioUnico extends RunnableAggregatorPerfomanceOld {
  val operationName = "DETTAGLIO_UNICO"

  def getCsvFields(dfAggregato: DataFrame): List[String]

  override val csvFields: List[String] = List()
  override val header: String = ""

  def getAggregato(df: DataFrame): DataFrame = throw new Exception("not supported")

  def fileSpecificFilterExpression: Column

  override def excludedPdrsSpecificCondition: Column = col(DailyConsumptionAggSchema.treatment).isNotNull and
    (col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull || col(DailyConsumptionAggSchema.rightMeasureLocalFile).isNotNull)

  // If something is modified here, it's important to modify it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.elencoFlussi.getAggregato as well
  // They accomplish the same task, but getAggregato in SBG module has to compute the consumption in a different way
  def getAggregato(df: DataFrame, validateFlow: DataFrame): DataFrame = {
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
      //.withColumn(DailyConsumptionAggSchema.value, when(col(DailyConsumptionInputProcessSchema.isDayAnomalous), col(DailyConsumptionAggSchema.valuef3)).otherwise(col(DailyConsumptionAggSchema.value)))
      .selectExpr(
        aggregatoColumns.keySet.toList.union(
          List(
            DailyConsumptionAggSchema.date.toString,
            DailyConsumptionAggSchema.rightMeasureLocalFile.toString,
            DailyConsumptionAggSchema.idFormula.toString)): _*
      )

    // We force valuef3 for excluded pdrs as well
    val excludedPdrs = getExcludedPdrs(df)
      //.withColumn(DailyConsumptionAggSchema.value, col(DailyConsumptionAggSchema.valuef3))
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
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    computeElencoFlussi(filteredDF, validateFlow)
  }

  /**
   * Esegue le query finalizzate all'estrazione delle'elenco flussi da [[validateFlow]] per i PdR presenti in [[filteredDF]].
   * @param filteredDF tabella dei consumi filtrata
   * @param validateFlow tabella delle misure da cui estrarre i flussi
   * @return l'elenco dei flussi relativi all'insieme di PdR da pubblicare
   */
  def computeElencoFlussi(filteredDF: DataFrame, validateFlow: DataFrame): DataFrame = {
    val pdrTmpName = "pdr_to_del"
    val annoMeseTmpName = "anno_mese_to_del"
    val leftFileTmpName = "left_file_to_del"

    val concatInArray = udf((col1: String, col2: String) => Array(col1, col2))

    //-------------------------------------------Recover local file in validated flow-----------------------------------------------------------------
    //        logger.warn(s"Count di filteredDF: "+ filteredDF.count())


    val formula4Condition = col(DailyConsumptionAggSchema.idFormula) === "4"

    validateFlow.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val filteredDFSelect = filteredDF.select(
      DailyConsumptionAggSchema.pdr
      , DailyConsumptionAggSchema.date
      , DailyConsumptionAggSchema.annoMese
      , DailyConsumptionAggSchema.leftMeasureLocalFile
      , DailyConsumptionAggSchema.rightMeasureLocalFile
      , DailyConsumptionAggSchema.idFormula
    ).filter(formula4Condition).select(
      DailyConsumptionAggSchema.pdr
      , DailyConsumptionAggSchema.date
      , DailyConsumptionAggSchema.annoMese
      , DailyConsumptionAggSchema.leftMeasureLocalFile
    )

    filteredDFSelect.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di filteredDFSelect: " + filteredDFSelect.count()) //not comment the log count for timeout error

    val formual4 = broadcast(filteredDFSelect)
      .join(
        validateFlow.select(
          ValidatedFlowsAggSchema.pdr, ValidatedFlowsAggSchema.date, ValidatedFlowsAggSchema.localFile
        )
        , filteredDFSelect(DailyConsumptionAggSchema.pdr) === validateFlow(ValidatedFlowsAggSchema.pdr) &&
          filteredDFSelect(DailyConsumptionAggSchema.date) === validateFlow(ValidatedFlowsAggSchema.date),
        "inner"
      )
      .drop(validateFlow(ValidatedFlowsAggSchema.pdr))
      .drop(validateFlow(ValidatedFlowsAggSchema.date))
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, explode(concatInArray(col(DailyConsumptionAggSchema.leftMeasureLocalFile), col(ValidatedFlowsAggSchema.localFile))))
      .filter(col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull)
      .drop(validateFlow(ValidatedFlowsAggSchema.localFile))
      .select(
        DailyConsumptionAggSchema.pdr
        , DailyConsumptionAggSchema.annoMese
        , DailyConsumptionAggSchema.leftMeasureLocalFile
      )
      .distinct()

    //    formual4.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di formual4: "+ formual4.count())

    val recoverRightMeasure = filteredDF
      .select(
        DailyConsumptionAggSchema.pdr
        , DailyConsumptionAggSchema.annoMese
        , DailyConsumptionAggSchema.leftMeasureLocalFile
        , DailyConsumptionAggSchema.rightMeasureLocalFile
      ).distinct()
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, explode(concatInArray(col(DailyConsumptionAggSchema.leftMeasureLocalFile), col(DailyConsumptionAggSchema.rightMeasureLocalFile))))
      .drop(DailyConsumptionAggSchema.rightMeasureLocalFile)
      .filter(col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull)
      .select(
        DailyConsumptionAggSchema.pdr
        , DailyConsumptionAggSchema.annoMese
        , DailyConsumptionAggSchema.leftMeasureLocalFile
      )
      .distinct()

    //    recoverRightMeasure.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di recoverRightMeasure: "+ recoverRightMeasure.count())

    val validateIm1Igmg = validateFlow
      .filter(upper(col(ValidatedFlowsAggSchema.service)).isin("IM1PRE", "IM1POST", "IGMGPRE", "IGMGPOST") && col(ValidatedFlowsAggSchema.isCorrected))
      .select(
        ValidatedFlowsAggSchema.pdr,
        ValidatedFlowsAggSchema.date
      )

    validateIm1Igmg.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di validateIm1Igmg: " + validateIm1Igmg.count()) //not comment the log count for timeout error


    val PDR_RETTIFICA = "PDR_RETTIFICA"
    val DATA_RETTIFICA = "DATA_RETTIFICA"

    val validateRglRml = validateFlow
      .filter(upper(col(ValidatedFlowsAggSchema.service)).isin("RGL", "RML"))
      .select(
        ValidatedFlowsAggSchema.pdr,
        ValidatedFlowsAggSchema.date,
        ValidatedFlowsAggSchema.localFile
      )
      .withColumnRenamed(ValidatedFlowsAggSchema.pdr, PDR_RETTIFICA)
      .withColumnRenamed(ValidatedFlowsAggSchema.date, DATA_RETTIFICA)


    val joinValidateFlow = broadcast(validateIm1Igmg)
      .join(validateRglRml,
        validateIm1Igmg(ValidatedFlowsAggSchema.pdr) === validateRglRml(PDR_RETTIFICA) &&
          validateIm1Igmg(DailyConsumptionAggSchema.date) === validateRglRml(DATA_RETTIFICA),
        "inner"
      )
      .select(
        ValidatedFlowsAggSchema.pdr,
        ValidatedFlowsAggSchema.date,
        ValidatedFlowsAggSchema.localFile
      )

    joinValidateFlow.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di joinValidateFlow: " + joinValidateFlow.count()) //not comment the log count for timeout error


    val recoverRettifiche = filteredDF.select(
      DailyConsumptionAggSchema.pdr
      , DailyConsumptionAggSchema.date
      , DailyConsumptionAggSchema.annoMese
      , DailyConsumptionAggSchema.leftMeasureLocalFile
    )
      .join(broadcast(joinValidateFlow),
        filteredDF(DailyConsumptionAggSchema.pdr) === joinValidateFlow(ValidatedFlowsAggSchema.pdr) &&
          filteredDF(DailyConsumptionAggSchema.date) === joinValidateFlow(ValidatedFlowsAggSchema.date),
        "inner"
      )
      .drop(joinValidateFlow(ValidatedFlowsAggSchema.pdr))
      .drop(joinValidateFlow(ValidatedFlowsAggSchema.date))
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, explode(concatInArray(col(DailyConsumptionAggSchema.leftMeasureLocalFile), col(ValidatedFlowsAggSchema.localFile))))
      .drop(joinValidateFlow(ValidatedFlowsAggSchema.localFile))
      .select(
        DailyConsumptionAggSchema.pdr
        , DailyConsumptionAggSchema.annoMese
        , DailyConsumptionAggSchema.leftMeasureLocalFile
      )
      .distinct()

    //    recoverRettifiche.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di recoverRettifiche: "+ recoverRettifiche.count())
    //    joinValidateFlow.unpersist()

    val uniondf = formual4
      .union(recoverRightMeasure).coalesce(3000)
      .union(recoverRettifiche).coalesce(3000)

    //    uniondf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di uniondf: "+ uniondf.count())
    //    formual4.unpersist()
    //    recoverRightMeasure.unpersist()
    //    recoverRettifiche.unpersist()

    //------------------------------------------------------------------------------------------------------------
    val fileDF = uniondf
      .filter(col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull)
      .select(DailyConsumptionAggSchema.pdr
        , DailyConsumptionAggSchema.annoMese
        , DailyConsumptionAggSchema.leftMeasureLocalFile)
      .distinct()
      .withColumnRenamed(DailyConsumptionAggSchema.pdr, pdrTmpName)
      .withColumnRenamed(DailyConsumptionAggSchema.annoMese, annoMeseTmpName)
      .withColumnRenamed(DailyConsumptionAggSchema.leftMeasureLocalFile, leftFileTmpName)

    //    fileDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di fileDF: "+ fileDF.count())
    //    uniondf.unpersist()

    val aggDF = computeAggregation(filteredDF)

    val joinExpr = (fileDF.col(pdrTmpName) === aggDF.col(DailyConsumptionAggSchema.pdr)) and (fileDF.col(annoMeseTmpName) === aggDF.col(DailyConsumptionAggSchema.annoMese))
    var joinedDf = aggDF.join(fileDF, joinExpr, "left")
      .withColumnRenamed(leftFileTmpName, DailyConsumptionAggSchema.leftMeasureLocalFile)
      .distinct
      //extracting a sub-path (leaf file + two ancestors) from full FS path
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, regexp_extract(col(DailyConsumptionAggSchema.leftMeasureLocalFile), "(\\/[0-9]{4}){2}\\/.*\\..*", 0))

    //    joinedDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di joinedDf: "+ joinedDf.count())
    //    fileDF.unpersist()

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      joinedDf = joinedDf.withColumn(fileName, col(dailyName).cast(StringType))
    })

    joinedDf
      .filter(col(keyPiva1).isNotNull && col(keyPiva2).isNotNull)
      .selectExpr(aggregatoColumns.values.toSeq: _*)
  }

  //In SBG this is overridden since we compute the aggregation at the beginning
  def computeAggregation(df: DataFrame): DataFrame = {
    df
      .groupBy(aggregatoColumns.keySet.toList.diff(List(DailyConsumptionAggSchema.value.toString, DailyConsumptionAggSchema.leftMeasureLocalFile.toString)).distinct.map(col): _*)
      .agg(round(sum(col(DailyConsumptionAggSchema.value)),3).cast(DoubleType).as(DailyConsumptionAggSchema.value))
  }

  override def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_${sessionName}_${annomese}_ElencoFlussi_${timestamp}_${counterCsv}.csv"
  }
}
