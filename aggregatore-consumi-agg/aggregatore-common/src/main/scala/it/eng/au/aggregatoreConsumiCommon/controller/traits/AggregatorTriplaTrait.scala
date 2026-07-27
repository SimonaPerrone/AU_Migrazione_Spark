package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.dettaglio.PdrDettaglioAggregatoTripla
import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.elencoRemi.ElencoRemiDettaglioAggregatoTripla
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, FileUtility}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.ZipOutputStream
import scala.collection.immutable.ListMap

trait AggregatorTriplaTrait extends RunnableAggregatorPerfomance {
  override val operationName = "DISTR_IT_REMI"
  override val annoMese: String = "annomese"

  val pdrDettaglioAggregatoTripla: PdrDettaglioAggregatoTripla
  val elencoFlussiDettaglioAggregatoTripla: ElencoRemiDettaglioAggregatoTripla

  /**
   * Esegue il processo di creazione CSV parallelamente per
   *  - l'insieme delle triple da pubblicare e i relativi consumi (CSV dei consumi);
   *  - l'insieme delle triple da pubblicare .
   *    Prima della scrittura dello ZIP, effettua l'union tra i due RDD in modo tale da inserire le due tipologie di CSV nello stesso ZIP.
   *    Infine scrive l'esito della pubblicazione nella tabella di reportistica.
   *
   * @param df tabella dei consumi
   */
  override def run(df: DataFrame): Unit = {
    val dfAggregatoPdr = pdrDettaglioAggregatoTripla.getAggregato(df)
    val dfAggregatoElencoFlussi = elencoFlussiDettaglioAggregatoTripla
      .getElencoRemi(dfAggregatoPdr, getAndPrepareRemiAnagrafica(), getAndPrepareGestTrasporto(), getAndPrepareVRcugasIt(), getAndPrepareConnessioniDistr())

    val dfAggregatoElencoFlussiForCsv = convertColumnsToString(dfAggregatoElencoFlussi).na.fill("")
    val dfAggregatoPdrForCsv = convertColumnsToString(dfAggregatoPdr).na.fill("")

    val fieldsElencoFlussi = elencoFlussiDettaglioAggregatoTripla.csvFields
    val fieldsPdr = pdrDettaglioAggregatoTripla.csvFields

    val csvOutputModelElencoFlussi = getCsvOutputModel(dfAggregatoElencoFlussiForCsv, fieldsElencoFlussi)
    val csvOutputModelPdr = getCsvOutputModel(dfAggregatoPdrForCsv, fieldsPdr)

    val rddInfoElencoFlussi = writeCsvAnnoMese(csvOutputModelElencoFlussi, fieldsElencoFlussi, elencoFlussiDettaglioAggregatoTripla)
    val rddInfoPdr = writeCsvAnnoMese(csvOutputModelPdr, fieldsPdr, pdrDettaglioAggregatoTripla)
    val rddInfo = rddInfoElencoFlussi.union(rddInfoPdr).coalesce(rddInfoPdr.getNumPartitions)
      .groupByKey()
      .mapValues(values => values.head)

    val rddWithInfo = writeZipAnnoMese(rddInfo)

    //    rddWithInfo.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddWithInfo: "+ rddWithInfo.count())

    writeInfoInTable(rddWithInfo)
    //    pdrDettaglioUnico.write(dfAggregatoPdr, fieldsPdr)
    //    elencoFlussiDettaglioUnico.write(dfAggregatoElencoFlussi, fieldsElencoFlussi)

    //    val rddInfo = mergeAndZipFiles(fieldsPdr, fieldsElencoFlussi)

    //    writeInfoInTable(rddInfo)
  }

  def writeCsvAnnoMese(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String], aggregator: RunnableAggregatorPerfomance): RDD[(String, String)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val publicationType = getPublicationType
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val sessionName = getSessionName
    val baseName = getPublicationType + baseNumber

    FileUtils.deleteDirectory(new File(tmpCsvOutput))

    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>
      val annomese = mapKeys(annoMese)
      val countCsv = mapKeys(counterCsv)
      val path = tmpCsvOutput + aggregator.getCsvOutputPath(baseName, mapKeys, daterun, publicationType, sessionName, countCsv, annomese)

      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      val header = publicationType match {
        case "AGG" => Some(columnsField.mkString(CSV_SEPARATOR))
        case "SBG" => Some(columnsField.map(_.toLowerCase).mkString(CSV_SEPARATOR))
      }

      FileUtility.writeCsv(path, header, records, appendMode = true)
      (mapKeys(mainPiva), path)
    }).groupByKey()
      .mapValues(values => values.head)
  }

  def writeZipAnnoMese(rddCsvPath: RDD[(String, String)]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val maxDimensionZip = getMaxSizeThresholdZip.toLong
    val executionId = getExecutionId
    val session = getSessionName
    val timestampToRun = Timestamp.valueOf(daterun)
    val publicationType = getPublicationType
    val baseName = getPublicationType + baseNumber

    val rddInfo = rddCsvPath.flatMap { case (pivaHead, path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(pivaHead, daterun, session)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val exist = if (outputFolderIfExists.exists() && outputFolderIfExists.canWrite) {
        val csvFiles = pathInputFileCsv.getParentFile.listFiles()
          .filter(f => f.isFile && f.getName.endsWith(".csv"))

        var zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))

        csvFiles.foreach { csvFile =>
          val readZip = new File(outputFolder + zipName)
          val dimensionZipFile = readZip.length()
          if (dimensionZipFile < maxDimensionZip) {
            putIntoZip(zip, csvFile)
          }
          else {
            zip.close()
            count += 1
            zipName = originalZipName.replace("_1.zip", "_" + count + ".zip")
            zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
            putIntoZip(zip, csvFile)
          }
        }

        zip.close()
        ""
      } else {
        logger.warn(s"Couldn't write to $outputFolder, the path does not exist.")
        s" Couldn't write to ${outputFolder + originalZipName}, the path does not exist."
      }

      val result = (1 to count).toList.map { num =>
        (executionId, operationName, baseName,
          if (exist == "") outputFolder + originalZipName.replace("_1.zip", s"_$num.zip")
          else exist,
          timestampToRun, timestampToRun.getTime)
      }

      result
    }

    rddInfo
  }


  def getZipOutputName(pivaFolder: String, today: LocalDateTime, session: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: <PIVARdB_AGG_Sx_FIN_DISTR_IT_REMI_Timestamp_progressivo.zip>
    val zipName = s"/${pivaFolder}_${session}_${operationName}_${timestamp}_1.zip"
    zipName
  }

  def getAndPrepareRemiAnagrafica(): DataFrame = {
    Environment.sqlContext.table(Environment.getRcugasRemiAnagraficaTableName)
  }

  def getAndPrepareGestTrasporto(): DataFrame = {
    Environment.sqlContext.table(Environment.getRcugasGestoreTrasportoTableName)
  }

  def getAndPrepareVRcugasIt(): DataFrame = {
    Environment.sqlContext.table(Environment.getRcugasVRcugasItTableName)
  }

  def getAndPrepareConnessioniDistr(): DataFrame = {
    Environment.sqlContext.table(Environment.getRcugasConnessioniDistrTableName)
  }

  //Not used
  override def getAggregato(df: DataFrame): DataFrame = Environment.sqlContext.emptyDataFrame

  override val aggregatoColumns: ListMap[String, String] = ListMap()
  override val csvFields: List[String] = List()
}