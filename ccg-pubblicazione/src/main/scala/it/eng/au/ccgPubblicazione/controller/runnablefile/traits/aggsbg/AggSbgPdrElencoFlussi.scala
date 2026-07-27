package it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg

import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.RunnableAggregator
import it.eng.au.ccgPubblicazione.dao.rcugas.ClassiGruppiDiMisuraPortataRcugas
import it.eng.au.ccgPubblicazione.schema.aggsbg.output.{AggConsumiOutputSchema, AggFlussiOutputSchema}
import it.eng.au.ccgPubblicazione.schema.aggsbg.{AggConsumptionRequestRunnableSchema, ValidatedFlowsAggSchema}
import it.eng.au.ccgPubblicazione.utility.Constants.DETTAGLIOUNICO
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListMap

trait AggSbgPdrElencoFlussi extends RunnableAggregator {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  // Used for incoerenti GDM
  val isPdrAnomalousGDM = "is_pdr_anomalous_gdm"

  /** Filtro presente in [[getAnomalousPdrs]] per permettere ai processi di AGG/SBG di aggiungere un filtro specifico per gli incoerenti GDM.
   *  - in AGG vengono pubblicati tutti i tipi;
   *  - in SBG solo i trattamenti "G" e "M".
   *  */
  def specificFilterForIncoerentiGdm: Column = lit(true)

  override val publicationType: String = DETTAGLIOUNICO
  override val aggregatoColumnsConsumi: ListMap[String, String] = ListMap(
//    AggConsumptionRequestRunnableSchema.annoMese.toString -> AggConsumiOutputSchema.data.toString,
    AggConsumptionRequestRunnableSchema.pdr.toString -> AggConsumiOutputSchema.cod_pdr.toString,
    AggConsumptionRequestRunnableSchema.pivaDistr.toString -> AggConsumiOutputSchema.piva_distr.toString,
    AggConsumptionRequestRunnableSchema.pivaIt.toString -> AggConsumiOutputSchema.piva_it.toString,
    AggConsumptionRequestRunnableSchema.pivaUdd.toString -> AggConsumiOutputSchema.piva_udd.toString,
    AggConsumptionRequestRunnableSchema.pivaUdb.toString -> AggConsumiOutputSchema.piva_udb.toString,
    AggConsumptionRequestRunnableSchema.dtg.toString -> AggConsumiOutputSchema.dtg.toString,
    AggConsumptionRequestRunnableSchema.codRemi.toString -> AggConsumiOutputSchema.cod_remi.toString,
    AggConsumptionRequestRunnableSchema.ca.toString -> AggConsumiOutputSchema.prel_annuo_prev.toString,
    AggConsumptionRequestRunnableSchema.idRegClim.toString -> AggConsumiOutputSchema.id_reg_clim.toString,
    AggConsumptionRequestRunnableSchema.codProfStd.toString -> AggConsumiOutputSchema.cod_prof_prel_std.toString,
    AggConsumptionRequestRunnableSchema.treatment.toString -> AggConsumiOutputSchema.trattamento.toString,
    AggConsumptionRequestRunnableSchema.tipoCliente.toString -> AggConsumiOutputSchema.tipo_cliente.toString,
    AggConsumptionRequestRunnableSchema.unitMisPrel.toString -> AggConsumiOutputSchema.un_mis_prel.toString,
    AggConsumptionRequestRunnableSchema.classeMisuratore.toString -> AggConsumiOutputSchema.classe_gruppo_mis.toString/*,
    AggConsumptionRequestRunnableSchema.idRichiesta.toString -> AggConsumiOutputSchema.id_richiesta.toString,
    AggConsumptionRequestRunnableSchema.dataRichiesta.toString -> AggConsumiOutputSchema.data_richiesta.toString*/
  )

  override val aggregatoColumnsFlussi: ListMap[String, String] = ListMap(
    AggConsumptionRequestRunnableSchema.pdr.toString -> AggFlussiOutputSchema.pdr.toString,
    AggConsumptionRequestRunnableSchema.value.toString -> AggFlussiOutputSchema.prelievo.toString,
    AggConsumptionRequestRunnableSchema.treatment.toString -> AggFlussiOutputSchema.trattamento.toString,
    AggConsumptionRequestRunnableSchema.leftMeasureLocalFile.toString -> AggFlussiOutputSchema.path_cloud.toString,
    AggConsumptionRequestRunnableSchema.session.toString -> AggFlussiOutputSchema.sessione.toString,
    AggConsumptionRequestRunnableSchema.annoMese.toString -> AggFlussiOutputSchema.annomese.toString/*,
    AggConsumptionRequestRunnableSchema.idRichiesta.toString -> AggFlussiOutputSchema.id_richiesta.toString,
    AggConsumptionRequestRunnableSchema.dataRichiesta.toString -> AggFlussiOutputSchema.data_richiesta.toString*/
  )

  val dayOfMonth = "dayOfMonth"
  val dataValColName = "DATA_VAL"
  val pivotPrefix = "prelievo_giorn_"

  /** Mappa tra i campi chiave e le colonne presenti nel dataframe dei consumi. */
  val keyFiledsPreRenamed: ListMap[String, String]

  override val headerCsvConsumi: List[String] = AggConsumiOutputSchema.getValues ::: (1 to 31).map(pivotPrefix + _).toList
  override val headerCsvFlussi: List[String] = AggFlussiOutputSchema.getValues
  override val pdrField: String = AggConsumiOutputSchema.cod_pdr.toString
  override val dataField: String = AggConsumiOutputSchema.data.toString

  def getAnomalousPdrs(df: DataFrame): DataFrame = {
    df.where(col(AggConsumptionRequestRunnableSchema.isPdrAnomalousGDM) and specificFilterForIncoerentiGdm and fileSpecificFilterExpression)
  }

  def getExcludedPdrs(df: DataFrame): DataFrame = {
    df.where(col(AggConsumptionRequestRunnableSchema.esclusiFlag))
  }

  @deprecated
  def findAnomalousDays(df: DataFrame): DataFrame = {
    val thresholdAnomalousDays = Environment.getNumberOfDaysThresholdForGdm.toInt
    val windowByPdrInMonth = Window.partitionBy(col(AggConsumptionRequestRunnableSchema.pdr), col(AggConsumptionRequestRunnableSchema.annoMese))
    val anomalousDaysCount = "anomalous_days_count"

    val classiGdmToPortataMaxMap = ClassiGruppiDiMisuraPortataRcugas.getAsMap()
    val classiGdmToPortataMax: UserDefinedFunction = udf((classeGdm: String) => classiGdmToPortataMaxMap.get(classeGdm))

    val portataMassima = classiGdmToPortataMax(col(AggConsumptionRequestRunnableSchema.classeMisuratore)) * coalesce(col(AggConsumptionRequestRunnableSchema.coefficient), lit(1.0))

    df
      .withColumn(AggConsumptionRequestRunnableSchema.isDayAnomalous, col(AggConsumptionRequestRunnableSchema.value) > portataMassima)
      //A PdR is anomalous GDM if and only if the number of anomalous days isn't greater than a given threshold (and greater than 0)
      .withColumn(anomalousDaysCount, sum(col(AggConsumptionRequestRunnableSchema.isDayAnomalous).cast(IntegerType)).over(windowByPdrInMonth))
      .withColumn(isPdrAnomalousGDM, when(col(anomalousDaysCount) > 0 and col(anomalousDaysCount) < thresholdAnomalousDays, true).otherwise(false))
      //Then, if the PdR is anomalous GDM, we keep the value of isDayAnomalous, otherwise we put it to false
      .withColumn(AggConsumptionRequestRunnableSchema.isDayAnomalous, when(col(isPdrAnomalousGDM), col(AggConsumptionRequestRunnableSchema.isDayAnomalous)).otherwise(false))
      .drop(anomalousDaysCount)
  }

  /**
   * Applica, al dataframe dei consumi [[df]], i filtri relativi alla pubblicazione da effettuare (e.g. PdR validi, trattamento != 'N', ...).
   * @param df dataframe dei consumi
   * @return [[df]] filtrato
   */
  def consumptionFilter(df: DataFrame): DataFrame = {
    val aggDf = df.na.fill("Y", Seq(AggConsumptionRequestRunnableSchema.treatment.toString))
      .filter(col(AggConsumptionRequestRunnableSchema.errorCode).isin(0, 10, 11, 12) and
        not(col(AggConsumptionRequestRunnableSchema.forceExclusion) <=> true) and
        (col(AggConsumptionRequestRunnableSchema.treatment) =!= "N") and
        col(AggConsumptionRequestRunnableSchema.isValid) === true and
        col(AggConsumptionRequestRunnableSchema.dtg).isNotNull and
        col(AggConsumptionRequestRunnableSchema.codRemi).isNotNull and
        col(AggConsumptionRequestRunnableSchema.codProfStd).isNotNull and
        col(AggConsumptionRequestRunnableSchema.tipoCliente).isNotNull and
        fileSpecificFilterExpression
      )

    val esclusiDf = getExcludedPdrs(df)

    aggDf.unionByName(esclusiDf)
  }

  override def getPdr(df: DataFrame): DataFrame = {
    var aggDF = consumptionFilter(df)
      .withColumn(dayOfMonth, concat(lit(pivotPrefix), dayofmonth(col(AggConsumptionRequestRunnableSchema.date))))
      .groupBy(aggregatoColumnsConsumi.keySet.toSeq.union(keyFiledsPreRenamed.values.toSeq).distinct.map(col): _*)
      .pivot(dayOfMonth, (1 to 31).map(pivotPrefix + _))
      .agg(round(sum(col(AggConsumptionRequestRunnableSchema.value))).cast(IntegerType))
      .withColumn(AggConsumiOutputSchema.data, date_format(trunc(to_date(unix_timestamp(col(AggConsumptionRequestRunnableSchema.annoMese), "yyyyMM").cast(TimestampType)), "month"), "dd/MM/yyyy"))

    aggregatoColumnsConsumi.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumn(fileName, col(dailyName).cast(StringType))
    })

    aggDF.filter(keyFieldsConsumi.values.map(f => col(f).isNotNull).reduce(_ && _))
      .selectExpr(headerCsvConsumi.union(keyFieldsConsumi.values.toSeq).distinct: _*).na.fill("")
  }

  override def getElencoFlussi(df: DataFrame, validate: DataFrame): DataFrame = {
    val pdrTmpName = "pdr_to_del"
    val annoMeseTmpName = "anno_mese_to_del"
    val leftFileTmpName = "left_file_to_del"

    val concatInArray = udf((col1: String, col2: String) => Array(col1, col2))

    val filteredDF = consumptionFilter(df)
      .filter(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile).isNotNull || col(AggConsumptionRequestRunnableSchema.rightMeasureLocalFile).isNotNull)
      .selectExpr(
        aggregatoColumnsFlussi.keySet.toList.union(
          List(
            AggConsumptionRequestRunnableSchema.date.toString,
            AggConsumptionRequestRunnableSchema.rightMeasureLocalFile.toString,
            AggConsumptionRequestRunnableSchema.idFormula.toString
          )
        ).union(keyFiledsPreRenamed.values.toSeq).distinct
          : _*
      )
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //-------------------------------------------Recover local file in validated flow-----------------------------------------------------------------
    //        logger.warn(s"Count di filteredDF: "+ filteredDF.count())


    val formula4Condition = col(AggConsumptionRequestRunnableSchema.idFormula) === "4"

    validate.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val filteredDFSelect = filteredDF.select(
      AggConsumptionRequestRunnableSchema.pdr
      , AggConsumptionRequestRunnableSchema.date
      , AggConsumptionRequestRunnableSchema.annoMese
      , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile
      , AggConsumptionRequestRunnableSchema.rightMeasureLocalFile
      , AggConsumptionRequestRunnableSchema.idFormula
    ).filter(formula4Condition).select(
      AggConsumptionRequestRunnableSchema.pdr
      , AggConsumptionRequestRunnableSchema.date
      , AggConsumptionRequestRunnableSchema.annoMese
      , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile
    )

    filteredDFSelect.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di filteredDFSelect: " + filteredDFSelect.count()) //not comment the log count for timeout error

    val formual4 = broadcast(filteredDFSelect)
      .join(
        validate.select(
          ValidatedFlowsAggSchema.pdr, ValidatedFlowsAggSchema.date, ValidatedFlowsAggSchema.localfile
        )
        , filteredDFSelect(AggConsumptionRequestRunnableSchema.pdr) === validate(ValidatedFlowsAggSchema.pdr) &&
          filteredDFSelect(AggConsumptionRequestRunnableSchema.date) === validate(ValidatedFlowsAggSchema.date),
        "inner"
      )
      .drop(validate(ValidatedFlowsAggSchema.pdr))
      .drop(validate(ValidatedFlowsAggSchema.date))
      .withColumn(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile, explode(concatInArray(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile), col(ValidatedFlowsAggSchema.localfile))))
      .filter(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile).isNotNull)
      .drop(validate(ValidatedFlowsAggSchema.localfile))
      .select(
        AggConsumptionRequestRunnableSchema.pdr
        , AggConsumptionRequestRunnableSchema.annoMese
        , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile
      )
      .distinct()

    //    formual4.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di formual4: "+ formual4.count())

    val recoverRightMeasure = filteredDF
      .select(
        AggConsumptionRequestRunnableSchema.pdr
        , AggConsumptionRequestRunnableSchema.annoMese
        , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile
        , AggConsumptionRequestRunnableSchema.rightMeasureLocalFile
      ).distinct()
      .withColumn(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile, explode(concatInArray(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile), col(AggConsumptionRequestRunnableSchema.rightMeasureLocalFile))))
      .drop(AggConsumptionRequestRunnableSchema.rightMeasureLocalFile)
      .filter(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile).isNotNull)
      .select(
        AggConsumptionRequestRunnableSchema.pdr
        , AggConsumptionRequestRunnableSchema.annoMese
        , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile
      )
      .distinct()

    //    recoverRightMeasure.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di recoverRightMeasure: "+ recoverRightMeasure.count())

    val validateIm1Igmg = validate
      .filter(upper(col(ValidatedFlowsAggSchema.service)).isin("IM1PRE", "IM1POST", "IGMGPRE", "IGMGPOST") && col(ValidatedFlowsAggSchema.iscorrected))
      .select(
        ValidatedFlowsAggSchema.pdr,
        ValidatedFlowsAggSchema.date
      )

    validateIm1Igmg.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di validateIm1Igmg: " + validateIm1Igmg.count()) //not comment the log count for timeout error


    val PDR_RETTIFICA = "PDR_RETTIFICA"
    val DATA_RETTIFICA = "DATA_RETTIFICA"

    val validateRglRml = validate
      .filter(upper(col(ValidatedFlowsAggSchema.service)).isin("RGL", "RML"))
      .select(
        ValidatedFlowsAggSchema.pdr,
        ValidatedFlowsAggSchema.date,
        ValidatedFlowsAggSchema.localfile
      )
      .withColumnRenamed(ValidatedFlowsAggSchema.pdr, PDR_RETTIFICA)
      .withColumnRenamed(ValidatedFlowsAggSchema.date, DATA_RETTIFICA)


    val joinvalidate = broadcast(validateIm1Igmg)
      .join(validateRglRml,
        validateIm1Igmg(ValidatedFlowsAggSchema.pdr) === validateRglRml(PDR_RETTIFICA) &&
          validateIm1Igmg(AggConsumptionRequestRunnableSchema.date) === validateRglRml(DATA_RETTIFICA),
        "inner"
      )
      .select(
        ValidatedFlowsAggSchema.pdr,
        ValidatedFlowsAggSchema.date,
        ValidatedFlowsAggSchema.localfile
      )

    joinvalidate.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di joinvalidate: " + joinvalidate.count()) //not comment the log count for timeout error


    val recoverRettifiche = filteredDF.select(
      AggConsumptionRequestRunnableSchema.pdr
      , AggConsumptionRequestRunnableSchema.date
      , AggConsumptionRequestRunnableSchema.annoMese
      , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile
    )
      .join(broadcast(joinvalidate),
        filteredDF(AggConsumptionRequestRunnableSchema.pdr) === joinvalidate(ValidatedFlowsAggSchema.pdr) &&
          filteredDF(AggConsumptionRequestRunnableSchema.date) === joinvalidate(ValidatedFlowsAggSchema.date),
        "inner"
      )
      .drop(joinvalidate(ValidatedFlowsAggSchema.pdr))
      .drop(joinvalidate(ValidatedFlowsAggSchema.date))
      .withColumn(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile, explode(concatInArray(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile), col(ValidatedFlowsAggSchema.localfile))))
      .drop(joinvalidate(ValidatedFlowsAggSchema.localfile))
      .select(
        AggConsumptionRequestRunnableSchema.pdr
        , AggConsumptionRequestRunnableSchema.annoMese
        , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile
      )
      .distinct()

    //    recoverRettifiche.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di recoverRettifiche: "+ recoverRettifiche.count())
    //    joinvalidate.unpersist()

    val uniondf = formual4
      .unionByName(recoverRightMeasure).coalesce(filteredDF.rdd.getNumPartitions)
      .unionByName(recoverRettifiche).coalesce(filteredDF.rdd.getNumPartitions)

    //    uniondf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di uniondf: "+ uniondf.count())
    //    formual4.unpersist()
    //    recoverRightMeasure.unpersist()
    //    recoverRettifiche.unpersist()

    //------------------------------------------------------------------------------------------------------------
    val fileDF = uniondf
      .filter(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile).isNotNull)
      .select(AggConsumptionRequestRunnableSchema.pdr
        , AggConsumptionRequestRunnableSchema.annoMese
        , AggConsumptionRequestRunnableSchema.leftMeasureLocalFile)
      .distinct()
      .withColumnRenamed(AggConsumptionRequestRunnableSchema.pdr, pdrTmpName)
      .withColumnRenamed(AggConsumptionRequestRunnableSchema.annoMese, annoMeseTmpName)
      .withColumnRenamed(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile, leftFileTmpName)

    //    fileDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di fileDF: "+ fileDF.count())
    //    uniondf.unpersist()

    val aggDF = filteredDF
      .groupBy(aggregatoColumnsFlussi.keySet.toList.union(keyFiledsPreRenamed.values.toSeq).distinct.diff(List(AggConsumptionRequestRunnableSchema.value.toString, AggConsumptionRequestRunnableSchema.leftMeasureLocalFile.toString)).distinct.map(col): _*)
      .agg(round(sum(col(AggConsumptionRequestRunnableSchema.value))).cast(IntegerType).as(AggConsumptionRequestRunnableSchema.value))

    val joinExpr = (fileDF.col(pdrTmpName) === aggDF.col(AggConsumptionRequestRunnableSchema.pdr)) and (fileDF.col(annoMeseTmpName) === aggDF.col(AggConsumptionRequestRunnableSchema.annoMese))
    var joinedDf = aggDF.join(fileDF, joinExpr, "left")
      .withColumnRenamed(leftFileTmpName, AggConsumptionRequestRunnableSchema.leftMeasureLocalFile)
      .distinct
      //extracting a sub-path (leaf file + two ancestors) from full FS path
      .withColumn(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile, regexp_extract(col(AggConsumptionRequestRunnableSchema.leftMeasureLocalFile), "(\\/[0-9]{4}){2}\\/.*\\..*", 0))

    //    joinedDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di joinedDf: "+ joinedDf.count())
    //    fileDF.unpersist()

    aggregatoColumnsFlussi.foreach({ case (dailyName, fileName) =>
      joinedDf = joinedDf.withColumn(fileName, col(dailyName).cast(StringType))
    })

    joinedDf
      .filter(keyFieldsFlussi.values.map(f => col(f).isNotNull).reduce(_ && _))
      .selectExpr(headerCsvFlussi.union(keyFieldsFlussi.values.toSeq.union(List(keyFieldsFlussi(piva)))).distinct: _*)
      .na.fill("")
  }
}
