package it.eng.au.queryReport.query.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, SchemaEnum}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.elencoFlussi.ElencoFlussiDettaglioUnicoSbg
import it.eng.au.queryReport.query.traits.QueryTrait
import it.eng.au.queryReport.schema.{ElencoFlussiDUQuerySchema, ValidatedFlowsSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import scala.collection.immutable.ListMap

/** Esegue la query della pubblicazione definita in [[ElencoFlussiDettaglioUnicoSbg]]. */
object QueryElencoFlussiDettaglioUnico extends QueryTrait with ElencoFlussiDettaglioUnicoSbg {
  override val queryName = "elencoFlussoDettaglioUnico"
  override val tableName: String = Environment.getElencoFlussiDettaglioUnicoTableName

  val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> ElencoFlussiDUQuerySchema.pdr.toString,
    DailyConsumptionAggSchema.value.toString -> ElencoFlussiDUQuerySchema.prelievo.toString,
    DailyConsumptionAggSchema.treatment.toString -> ElencoFlussiDUQuerySchema.trattamento.toString,
    DailyConsumptionAggSchema.leftMeasureLocalFile.toString -> ElencoFlussiDUQuerySchema.nome_file.toString,
    DailyConsumptionAggSchema.session.toString -> ElencoFlussiDUQuerySchema.sessione.toString,
    DailyConsumptionAggSchema.annoMese.toString -> ElencoFlussiDUQuerySchema.annomese.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> ElencoFlussiDUQuerySchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> ElencoFlussiDUQuerySchema.piva_udb.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> ElencoFlussiDUQuerySchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaRdb.toString -> ElencoFlussiDUQuerySchema.piva_rdb.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> ElencoFlussiDUQuerySchema.piva_it.toString
  )

  override val outputSchema: SchemaEnum = ElencoFlussiDUQuerySchema

  override def hdfsOutputPath: String = Environment.getQueryElencoFlussiDettUniHdfsPath

  def getQueryDF(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    getAggregato(df, validateFlow)
      .withColumn(ElencoFlussiDUQuerySchema.dailyconsumption_executionid, lit(Environment.getDailyConsumptionExecutionid).cast(LongType))
      .withColumn(ElencoFlussiDUQuerySchema.executionid, lit(Timestamp.valueOf(Environment.getDateRun).getTime))
      .selectExpr(outputSchema.getValues: _*)
  }

  override def getAggregato(df: DataFrame, validateFlow: DataFrame): DataFrame = {
    val pdrTmpName = "pdr_to_del"
    val annoMeseTmpName = "anno_mese_to_del"
    val leftFileTmpName = "left_file_to_del"

    val concatInArray = udf((col1: String, col2: String) => Array(col1, col2))
    val listOfFieldToCsv = aggregatoColumns.keySet.toList.diff(List(DailyConsumptionAggSchema.value.toString, DailyConsumptionAggSchema.leftMeasureLocalFile.toString)).distinct
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
      .withColumn(DailyConsumptionAggSchema.value, round(sum(col(DailyConsumptionAggSchema.value)).over(windowSumValue),3).cast(DoubleType))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

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
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.leftMeasureLocalFile
    )

    filteredDFSelect.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di filteredDFSelect: " + filteredDFSelect.count()) //not comment the log count for timeout error

    val formual4 = broadcast(filteredDFSelect)
      .join(
        validateFlow.select(
          ValidatedFlowsSchema.pdr,
          ValidatedFlowsSchema.date,
          ValidatedFlowsSchema.localfile,
          ValidatedFlowsSchema.service,
          ValidatedFlowsSchema.readtype,
          ValidatedFlowsSchema.motivation,
          ValidatedFlowsSchema.serialnumbermis,
          ValidatedFlowsSchema.serialnumberconv,
          ValidatedFlowsSchema.measure,
          ValidatedFlowsSchema.converted,
          ValidatedFlowsSchema.cauintmis,
          ValidatedFlowsSchema.cauintcorr
        )
        , filteredDFSelect(DailyConsumptionAggSchema.pdr) === validateFlow(ValidatedFlowsSchema.pdr) &&
          filteredDFSelect(DailyConsumptionAggSchema.date) === validateFlow(ValidatedFlowsSchema.date),
        "inner"
      )
      .drop(validateFlow(ValidatedFlowsSchema.pdr))
      .drop(validateFlow(ValidatedFlowsSchema.date))
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, explode(concatInArray(col(DailyConsumptionAggSchema.leftMeasureLocalFile), col(ValidatedFlowsSchema.localfile))))
      .filter(col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull)
      .drop(validateFlow(ValidatedFlowsSchema.localfile))
      .select(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.annoMese,
        DailyConsumptionAggSchema.leftMeasureLocalFile,
        ValidatedFlowsSchema.service,
        ValidatedFlowsSchema.readtype,
        ValidatedFlowsSchema.motivation,
        ValidatedFlowsSchema.serialnumbermis,
        ValidatedFlowsSchema.serialnumberconv,
        ValidatedFlowsSchema.measure,
        ValidatedFlowsSchema.converted,
        ValidatedFlowsSchema.cauintmis,
        ValidatedFlowsSchema.cauintcorr
      )
      .distinct()

    //    formual4.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di formual4: "+ formual4.count())

    val recoverRightMeasure = filteredDF
      .select(
        DailyConsumptionAggSchema.pdr
        , DailyConsumptionAggSchema.date
        , DailyConsumptionAggSchema.annoMese
        , DailyConsumptionAggSchema.leftMeasureLocalFile
        , DailyConsumptionAggSchema.rightMeasureLocalFile
      ).join(validateFlow.select(
      ValidatedFlowsSchema.pdr,
      ValidatedFlowsSchema.date,
      ValidatedFlowsSchema.localfile,
      ValidatedFlowsSchema.service,
      ValidatedFlowsSchema.readtype,
      ValidatedFlowsSchema.motivation,
      ValidatedFlowsSchema.serialnumbermis,
      ValidatedFlowsSchema.serialnumberconv,
      ValidatedFlowsSchema.measure,
      ValidatedFlowsSchema.converted,
      ValidatedFlowsSchema.cauintmis,
      ValidatedFlowsSchema.cauintcorr
    )
      , filteredDFSelect(DailyConsumptionAggSchema.pdr) === validateFlow(ValidatedFlowsSchema.pdr) &&
        filteredDFSelect(DailyConsumptionAggSchema.date) === validateFlow(ValidatedFlowsSchema.date),
      "left"
    )
      .drop(validateFlow(ValidatedFlowsSchema.pdr))
      .drop(validateFlow(ValidatedFlowsSchema.date))
      .drop(filteredDF(DailyConsumptionAggSchema.date))
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, explode(concatInArray(col(DailyConsumptionAggSchema.leftMeasureLocalFile), col(DailyConsumptionAggSchema.rightMeasureLocalFile))))
      .drop(DailyConsumptionAggSchema.rightMeasureLocalFile)
      .filter(col(DailyConsumptionAggSchema.leftMeasureLocalFile).isNotNull)
      .select(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.annoMese,
        DailyConsumptionAggSchema.leftMeasureLocalFile,
        ValidatedFlowsSchema.service,
        ValidatedFlowsSchema.readtype,
        ValidatedFlowsSchema.motivation,
        ValidatedFlowsSchema.serialnumbermis,
        ValidatedFlowsSchema.serialnumberconv,
        ValidatedFlowsSchema.measure,
        ValidatedFlowsSchema.converted,
        ValidatedFlowsSchema.cauintmis,
        ValidatedFlowsSchema.cauintcorr
      )
      .distinct()

    //    recoverRightMeasure.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di recoverRightMeasure: "+ recoverRightMeasure.count())

    val validateIm1Igmg = validateFlow
      .filter(upper(col(ValidatedFlowsSchema.service)).isin("IM1PRE", "IM1POST", "IGMGPRE", "IGMGPOST") && col(ValidatedFlowsSchema.iscorrected))
      .select(
        ValidatedFlowsSchema.pdr,
        ValidatedFlowsSchema.date,
        ValidatedFlowsSchema.service,
        ValidatedFlowsSchema.readtype,
        ValidatedFlowsSchema.motivation,
        ValidatedFlowsSchema.serialnumbermis,
        ValidatedFlowsSchema.serialnumberconv,
        ValidatedFlowsSchema.measure,
        ValidatedFlowsSchema.converted,
        ValidatedFlowsSchema.cauintmis,
        ValidatedFlowsSchema.cauintcorr
      )

    validateIm1Igmg.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Count di validateIm1Igmg: " + validateIm1Igmg.count()) //not comment the log count for timeout error


    val PDR_RETTIFICA = "PDR_RETTIFICA"
    val DATA_RETTIFICA = "DATA_RETTIFICA"

    val validateRglRml = validateFlow
      .filter(upper(col(ValidatedFlowsSchema.service)).isin("RGL", "RML"))
      .select(
        ValidatedFlowsSchema.pdr,
        ValidatedFlowsSchema.date,
        ValidatedFlowsSchema.localfile
      )
      .withColumnRenamed(ValidatedFlowsSchema.pdr, PDR_RETTIFICA)
      .withColumnRenamed(ValidatedFlowsSchema.date, DATA_RETTIFICA)


    val joinValidateFlow = broadcast(validateIm1Igmg)
      .join(validateRglRml,
        validateIm1Igmg(ValidatedFlowsSchema.pdr) === validateRglRml(PDR_RETTIFICA) &&
          validateIm1Igmg(DailyConsumptionAggSchema.date) === validateRglRml(DATA_RETTIFICA),
        "inner"
      )
      .select(
        ValidatedFlowsSchema.pdr,
        ValidatedFlowsSchema.date,
        ValidatedFlowsSchema.localfile,
        ValidatedFlowsSchema.service,
        ValidatedFlowsSchema.readtype,
        ValidatedFlowsSchema.motivation,
        ValidatedFlowsSchema.serialnumbermis,
        ValidatedFlowsSchema.serialnumberconv,
        ValidatedFlowsSchema.measure,
        ValidatedFlowsSchema.converted,
        ValidatedFlowsSchema.cauintmis,
        ValidatedFlowsSchema.cauintcorr
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
        filteredDF(DailyConsumptionAggSchema.pdr) === joinValidateFlow(ValidatedFlowsSchema.pdr) &&
          filteredDF(DailyConsumptionAggSchema.date) === joinValidateFlow(ValidatedFlowsSchema.date),
        "inner"
      )
      .drop(joinValidateFlow(ValidatedFlowsSchema.pdr))
      .drop(joinValidateFlow(ValidatedFlowsSchema.date))
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, explode(concatInArray(col(DailyConsumptionAggSchema.leftMeasureLocalFile), col(ValidatedFlowsSchema.localfile))))
      .drop(joinValidateFlow(ValidatedFlowsSchema.localfile))
      .select(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.annoMese,
        DailyConsumptionAggSchema.leftMeasureLocalFile,
        ValidatedFlowsSchema.service,
        ValidatedFlowsSchema.readtype,
        ValidatedFlowsSchema.motivation,
        ValidatedFlowsSchema.serialnumbermis,
        ValidatedFlowsSchema.serialnumberconv,
        ValidatedFlowsSchema.measure,
        ValidatedFlowsSchema.converted,
        ValidatedFlowsSchema.cauintmis,
        ValidatedFlowsSchema.cauintcorr
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
      .select(
        DailyConsumptionAggSchema.pdr,
        DailyConsumptionAggSchema.annoMese,
        DailyConsumptionAggSchema.leftMeasureLocalFile,
        ValidatedFlowsSchema.service,
        ValidatedFlowsSchema.readtype,
        ValidatedFlowsSchema.motivation,
        ValidatedFlowsSchema.serialnumbermis,
        ValidatedFlowsSchema.serialnumberconv,
        ValidatedFlowsSchema.measure,
        ValidatedFlowsSchema.converted,
        ValidatedFlowsSchema.cauintmis,
        ValidatedFlowsSchema.cauintcorr
      )
      .distinct()
      .withColumnRenamed(DailyConsumptionAggSchema.pdr, pdrTmpName)
      .withColumnRenamed(DailyConsumptionAggSchema.annoMese, annoMeseTmpName)
      .withColumnRenamed(DailyConsumptionAggSchema.leftMeasureLocalFile, leftFileTmpName)

    //    fileDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di fileDF: "+ fileDF.count())
    //    uniondf.unpersist()

    val aggDF = filteredDF
      .select((listOfFieldToCsv :+ DailyConsumptionAggSchema.value.toString).map(col): _*)
      .distinct

    val joinExpr = (fileDF.col(pdrTmpName) === aggDF.col(DailyConsumptionAggSchema.pdr)) and (fileDF.col(annoMeseTmpName) === aggDF.col(DailyConsumptionAggSchema.annoMese))
    var joinedDf = aggDF.join(fileDF, joinExpr, "left")
      .withColumnRenamed(leftFileTmpName, DailyConsumptionAggSchema.leftMeasureLocalFile)
      .distinct
      //extracting a sub-path (leaf file + two ancestors) from full FS path
      .withColumn(DailyConsumptionAggSchema.leftMeasureLocalFile, regexp_extract(col(DailyConsumptionAggSchema.leftMeasureLocalFile), "(\\/[0-9]{4}){2}\\/.*\\..*", 0))

    //    joinedDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    logger.warn(s"Count di joinedDf: "+ joinedDf.count())
    //    fileDF.unpersist()

    aggregatoColumns.foreach({ case (dailyName, outputName) =>
      joinedDf = joinedDf.withColumnRenamed(dailyName, outputName)
    })

    joinedDf
      .withColumnRenamed(ValidatedFlowsSchema.service, ElencoFlussiDUQuerySchema.service)
      .withColumnRenamed(ValidatedFlowsSchema.readtype, ElencoFlussiDUQuerySchema.read_type)
      .withColumnRenamed(ValidatedFlowsSchema.motivation, ElencoFlussiDUQuerySchema.motivation)
      .withColumnRenamed(ValidatedFlowsSchema.serialnumbermis, ElencoFlussiDUQuerySchema.serial_number_mis)
      .withColumnRenamed(ValidatedFlowsSchema.serialnumberconv, ElencoFlussiDUQuerySchema.serial_number_conv)
      .withColumnRenamed(ValidatedFlowsSchema.measure, ElencoFlussiDUQuerySchema.measure)
      .withColumnRenamed(ValidatedFlowsSchema.converted, ElencoFlussiDUQuerySchema.converted)
      .withColumnRenamed(ValidatedFlowsSchema.cauintmis, ElencoFlussiDUQuerySchema.cau_int_mis)
      .withColumnRenamed(ValidatedFlowsSchema.cauintcorr, ElencoFlussiDUQuerySchema.cau_int_corr)
  }

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaDistr).isNotNull and
    col(DailyConsumptionAggSchema.pivaIt).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdb).isNotNull and
    col(DailyConsumptionAggSchema.pivaUdd).isNotNull and
    col(DailyConsumptionAggSchema.pivaRdb).isNotNull

  //Not needed
  override def getAggregato(df: DataFrame): DataFrame = df
  override def getCsvFields(dfAggregato: DataFrame): List[String] = List()
  override val keyPiva1: String = ""
  override val keyPiva2: String = ""
  override val baseNumber: String = ""
  override val mainPiva: String = ""
}