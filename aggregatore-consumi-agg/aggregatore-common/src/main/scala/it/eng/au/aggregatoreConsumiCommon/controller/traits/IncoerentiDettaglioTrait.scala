package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi.ElencoFlussiDettaglioIncoerenti
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.PdrDettaglioIncoerenti
import it.eng.au.aggregatoreConsumiCommon.schema.ValidatedFlowsAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, FileUtility}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.ZipOutputStream
import scala.collection.immutable.ListMap

trait IncoerentiDettaglioTrait extends RunnableAggregatorPerfomance {
  override val operationName = "INCOERENTI_GDM"

  // Needed because "annomese" is a field in the ElencoFlussi csv
  // In this way there is no ambiguity between annomese and annoMese in getOutputCsv method
  override val annoMese: String = "annomese"

  val pdrDettaglioIncoerenti: PdrDettaglioIncoerenti
  val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerenti

  /**
   * Esegue il processo di creazione CSV parallelamente per
   *  - l'insieme dei PdR da pubblicare e i relativi consumi (CSV dei consumi);
   *  - l'insieme dei PdR da pubblicare e i relativi flussi (CSV dell'elenco flussi).
   *  Prima della scrittura dello ZIP, effettua l'union tra i due RDD in modo tale da inserire le due tipologie di CSV nello stesso ZIP.
   *  Infine scrive l'esito della pubblicazione nella tabella di reportistica.
   * @param df tabella dei consumi
   */
  override def run(df: DataFrame): Unit = {
    val dfAggregatoElencoFlussi = elencoFlussiDettaglioIncoerenti.getAggregato(df, getAndPrepareValidateFlow())
    val dfAggregatoPdr = pdrDettaglioIncoerenti.getAggregato(df)

    val dfAggregatoElencoFlussiForCsv = convertColumnsToString(dfAggregatoElencoFlussi).na.fill("")
    val dfAggregatoPdrForCsv = convertColumnsToString(dfAggregatoPdr).na.fill("")

    // The diff in the following line is needed to avoid ambiguous columns
    val csvOutputModelElencoFlussi = getCsvOutputModel(dfAggregatoElencoFlussiForCsv, elencoFlussiDettaglioIncoerenti.csvFields)
    val csvOutputModelPdr = getCsvOutputModel(dfAggregatoPdrForCsv, pdrDettaglioIncoerenti.csvFields)

    val rddInfoElencoFlussi = writeCsvAnnoMese(csvOutputModelElencoFlussi, elencoFlussiDettaglioIncoerenti.csvFields, elencoFlussiDettaglioIncoerenti)
    val rddInfoPdr = writeCsvAnnoMese(csvOutputModelPdr, pdrDettaglioIncoerenti.csvFields, pdrDettaglioIncoerenti)

    val rddInfo = rddInfoElencoFlussi.union(rddInfoPdr).coalesce(rddInfoPdr.getNumPartitions)
      .groupByKey()
      .mapValues(values => values.head)

    val rddWithInfo = writeZipAnnoMese(rddInfo)

    writeInfoInTable(rddWithInfo)
  }

  def getAndPrepareValidateFlow(): DataFrame = {
    Environment.sqlContext.table(Environment.getValidatedFlowTableName)
      .selectExpr(ValidatedFlowsAggSchema.getValues: _*)
      .filter(col(ValidatedFlowsAggSchema.executionid) === Environment.getDailyConsumptionExecutionid)
      .drop(col(ValidatedFlowsAggSchema.executionid))
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
    val timestampToRun = Timestamp.valueOf(daterun)
    val publicationType = getPublicationType
    val baseName = getPublicationType + baseNumber
    val year = getYear

    val rddInfo = rddCsvPath.flatMap({ case (pivaHead, path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(pivaHead, publicationType, daterun, year)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val exist = if (outputFolderIfExists.exists() && outputFolderIfExists.canWrite) {
        var zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))

        pathInputFileCsv.getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".csv") && value.getName.contains(s"${pivaHead}_${publicationType}_${operationName}_")).foreach { csvFile =>
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
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits.")
        s" Couldn't write to ${outputFolder + originalZipName}, the path does not exits."
      }
      val result = (1 to count).toList.map { num =>
        (executionId, operationName, baseName,
          if (exist == "") outputFolder + originalZipName.replace("_1.zip", s"_$num.zip")
          else exist
          , timestampToRun, timestampToRun.getTime)
      }
      result
    })

    rddInfo
  }

  def getZipOutputName(pivaFolder: String, publicationType: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_(AGG_S1_PRE|SBG)_2022(04)_20220428105421_202204_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${publicationType}_${operationName}_${year}_${timestamp}_1.zip"
    zipName
  }

  //Not used
  override def getAggregato(df: DataFrame): DataFrame = Environment.sqlContext.emptyDataFrame

  override val aggregatoColumns: ListMap[String, String] = ListMap()
  override val csvFields: List[String] = List()
}
