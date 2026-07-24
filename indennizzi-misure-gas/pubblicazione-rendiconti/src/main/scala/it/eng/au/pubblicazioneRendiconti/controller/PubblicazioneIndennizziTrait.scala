package it.eng.au.pubblicazioneRendiconti.controller

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneRendiconti.model.ReportPubblicazioneRzg2
import it.eng.au.pubblicazioneRendiconti.utility.constants.Constants.{CSV_SEPARATOR, EURO, _EURO_SYMBOL_}
import it.eng.au.pubblicazioneRendiconti.utility.file.FileUtility
import it.eng.au.pubblicazioneRendiconti.utility.file.FileUtility.putIntoZip
import it.eng.au.pubblicazioneRendiconti.utility.properties.Properties
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ceil, col, count, monotonically_increasing_id}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.ZipOutputStream
import scala.collection.immutable.ListMap

trait PubblicazioneIndennizziTrait extends Serializable {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  val publicationType: String = "RZG2"
  val operationName: String = "CIG"
  val baseNumber: String = "2"
  val baseName: String = operationName + baseNumber

  def getTmpCsvOutput: String = Properties.getIsilonBasepathTmp + s"/tmp/$publicationType/$operationName/$operationName$baseNumber/$publicationType"

  def getPathZipOutput: String = Properties.getIsilonBasepathOut + s"/$operationName/$operationName$baseNumber"

  def getCsvMaxRowLength: Some[Long] = Some(Properties.getMaxNumRowFile.toLong)

  def getDateToRun: LocalDateTime = Environment.startDateTime

  def getExecutionId: String = Environment.executionId.toString

  val rzg2Columns: ListMap[String, String]
  val csvFields: List[String]
  val pivaID = "pivaID"
  val pivaUdd = "pivaUdd"
  val annomese = "annomese"
  val counterCsv = "counterCsv"
  val keyFields: ListMap[String, String]

  def getIndennizzi(df: DataFrame): DataFrame

  def getAndWriteIndennizzi(indennizziRzg2: DataFrame, partitionRead: String): RDD[ReportPubblicazioneRzg2] = {
    val df = convertColumnsToString(getIndennizzi(indennizziRzg2))
    val csvDf = getCsvOutputModel(df)
    val writecsv = writeCsv(csvDf, csvFields)
    writeZip(writecsv, partitionRead)
  }

  def convertColumnsToString(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((current, c) => current.withColumn(c, col(c).cast(StringType))).na.fill("")
  }

  def distribution(df: DataFrame, keys: List[String]): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get

    val window = Window.partitionBy(keys.map(col): _*)

    df
      .withColumn(counterCsv, count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col(counterCsv) / csvMaxNumberRow)) + 1).cast(StringType))
      .select(
        df("*"),
        col(counterCsv)
      )
      .repartition((keys :+ counterCsv).map(col): _*)
  }

  def getCsvOutputModel(df: DataFrame): RDD[(Map[String, String], Row)] = {
    var dfDistribution = distribution(df, keyFields.values.toList)

    val columns = df.columns :+ counterCsv

    columns.foreach(column =>
      dfDistribution = dfDistribution.withColumn(column, col(column).cast(StringType))
    )

    dfDistribution
      .rdd
      .map(row => {
        val keyMap = Map(
          pivaID -> row.getAs[String](keyFields(pivaID)),
          pivaUdd -> row.getAs[String](keyFields(pivaUdd)),
          annomese -> row.getAs[String](keyFields(annomese)),
          counterCsv -> row.getAs[String](counterCsv)
        )

        (keyMap, row)
      })
  }

  def writeCsv(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String]): RDD[(Map[String, String], String)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val daterun = getDateToRun

    FileUtils.deleteDirectory(new File(tmpCsvOutput)) //in windows test comment this
    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>

      val path = tmpCsvOutput + getCsvOutputPath(mapKeys, daterun)

      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      val header = Some(columnsField.mkString(CSV_SEPARATOR)
        .replace(_EURO_SYMBOL_, EURO))

      FileUtility.writeCsv(path, header, records, appendMode = true)

      (mapKeys, path)
    }).groupByKey()
      .mapValues(_.head)
  }

  def getCsvOutputPath(mapKey: Map[String, String], date: LocalDateTime): String = {
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaid = mapKey(pivaID)
    val pivaudd = mapKey(pivaUdd)
    val annoMese = mapKey(annomese)
    val countercsv = mapKey(counterCsv)
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)

    s"/${baseName}_${pivaudd}/$year/$month/${baseName}_${pivaudd}_${pivaid}/${pivaudd}_${pivaid}_${annoMese}_${publicationType}_${timestamp}_${countercsv}.csv"
  }

  def writeZip(rddCsvPath: RDD[(Map[String, String], String)], partitionRead: String): RDD[ReportPubblicazioneRzg2] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = Environment.startDateTime
    val executionId = partitionRead
    val timestampToRun = Timestamp.valueOf(daterun)

    val rddInfo = rddCsvPath.map({ case (mapKeys, path) =>
      val csvFile = new File(path)
      val outputFolder = csvFile.getParentFile.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      val zipName = getZipOutputName(mapKeys, daterun)
      val outputFolderIfExists = new File(outputFolder)

      val exist = if (outputFolderIfExists.exists() && outputFolderIfExists.canWrite) {
        val zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
        putIntoZip(zip, csvFile)
        zip.close()
        ""
      } else {
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits or permission are not set properly.")
        s" Couldn't write to ${outputFolder + zipName}, the path does not exits or permission are not set properly."
      }

      val result = ReportPubblicazioneRzg2(
          input_table_execution_id = executionId,
          operation_name = publicationType,
          base_name = baseName,
          path_name = if (exist == "") outputFolder + zipName else exist,
          load_date = timestampToRun,
          annomese = mapKeys(annomese)
        )

      result
    })

    rddInfo
  }

  //def writeZip(rddCsvPath: RDD[(Map[String, String], String)]): RDD[ReportPubblicazioneRzg2] = {
  //  val csvFileRegex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_(RZG2)_(\\d+)_(\\d+)\\.csv".r
  //  val tmpCsvOutput = getTmpCsvOutput
  //  val pathZipOutput = getPathZipOutput
  //  val daterun = Environment.startDateTime
  //  val executionId = Properties.getCigIndennizziRzg2ExecutionId
  //  val timestampToRun = Timestamp.valueOf(daterun)
  //
  //  val rddInfo = rddCsvPath.flatMap({ case (mapKeys, path) =>
  //    val pathInputFileCsv = new File(path)
  //    val outputFolder = pathInputFileCsv.getParentFile.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
  //    var zipName = getZipOutputName(mapKeys, daterun)
  //    val originalZipName = zipName
  //    val outputFolderIfExists = new File(outputFolder)
  //
  //    var count = 0
  //    val exist = if (outputFolderIfExists.exists() && outputFolderIfExists.canWrite) {
  //      pathInputFileCsv.getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".csv")).foreach { csvFile =>
  //        val counterCsv = csvFile.getName match { case csvFileRegex(pivaUdd, pivaId, yearMonth, rzg, timestamp, counter) => counter}
  //        count += 1
  //        zipName = originalZipName.replace("_1.zip", "_" + counterCsv + ".zip")
  //        val zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
  //        putIntoZip(zip, csvFile)
  //        zip.close()
  //      }
  //      ""
  //    } else {
  //      logger.warn(s"Couldn't write to $outputFolder, the path does not exits or permission are not set properly.")
  //      s" Couldn't write to ${outputFolder + originalZipName}, the path does not exits or permission are not set properly."
  //    }
  //
  //    val result = (1 to count).toList.map { num =>
  //      ReportPubblicazioneRzg2(
  //        input_table_execution_id = executionId,
  //        operation_name = publicationType,
  //        base_name = baseName,
  //        path_name = if (exist == "") outputFolder + originalZipName.replace("_1.zip", s"_$num.zip") else exist,
  //        load_date = timestampToRun,
  //        annomese = mapKeys(annomese)
  //      )
  //    }
  //    result
  //  })
  //
  //  rddInfo
  //}

  def getZipOutputName(mapKey: Map[String, String], today: LocalDateTime): String = {
    val pivaid = mapKey(pivaID)
    val pivaudd = mapKey(pivaUdd)
    val annoMese = mapKey(annomese)
    val csvCounter = mapKey(counterCsv)
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val zipName = s"/${pivaudd}_${pivaid}_${annoMese}_${publicationType}_${timestamp}_${csvCounter}.zip"
    zipName
  }
}
