package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.schema.InfoOutputSchema
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, FileUtility}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.immutable.ListMap

@deprecated("Use RunnableAggregatorPerfomance")
trait RunnableAggregatorPerfomanceOld extends RunnableAggregatorTrait {

  val CSV_SEPARATOR = ";"

  val keyPiva1: String
  val keyPiva2: String
  val mainPiva: String
  val counterCsv: String = "counterCsv"
  val aggregatoColumns: ListMap[String, String]
  val baseNumber: String
  val operationName: String
  //val firstHeader: String
  val header: String //= firstHeader + "\n" + CsvOutputModel.header
  val writeCsvHeader: Boolean = true
  val csvFields: List[String]

  def getAggregato(df: DataFrame): DataFrame

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def run(df: DataFrame): Unit = {

    val aggregato = getAggregato(df)
    val aggregatoForCsv = convertColumnsToString(aggregato).na.fill("")

    val csvOutputModel = getCsvOutputModel(aggregatoForCsv, csvFields)
    //    aggregato.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di aggregato: "+ aggregato.count())

    val rddCsvPath = writeCsv(csvOutputModel, csvFields)

    //    rddCsvPath.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddCsvPath: "+ rddCsvPath.count())
    val rddWithInfo = writeZip(rddCsvPath)

    //    rddWithInfo.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddWithInfo: "+ rddWithInfo.count())

    writeInfoInTable(rddWithInfo)

  }

  /**
   * the method write csv with two header ex:
   * PIVA_UDD	PIVA_DISTR	ANNO_COMPETENZA
   * COD_PDR	COD_REMI	CAT_USO	CLASSE_PRELIEVO
   * ----------------------------------------------------
   * codicepdr2	codiceremi2	catuso2	classeprelievo2
   * codicepdr3	codiceremi3	catuso3	classeprelievo3
   * codicepdr4	codiceremi4	catuso4	classeprelievo4
   * codicepdr5	codiceremi5	catuso5	classeprelievo5
   *
   * @param rddCsvOutput
   * @param daterun
   * @param prop
   * @param sc
   * @return rdd of (piva,path) unique because i need the parent path
   *         Ex. | piva  | path
   *         --------------------------------------
   *         | piva1 | piva1/piva1_pivadistr1.csv
   *         | piva1 | piva1/piva1_pivadistr2.csv
   *         | piva1 | piva1/piva1_pivadistr3.csv
   *         | piva1 | piva1/piva1_pivadistr4.csv
   *
   *         return
   *         | piva  | path
   *         --------------------------------------
   *         | piva1 | piva1/piva1_pivadistr1.csv
   */
  def writeCsv(rddCsvOutput: RDD[(ListMap[String, String], Row)], columnsField: List[String]): RDD[(String, String)] = {

    val tmpCsvOutput = getTmpCsvOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val baseName = getPublicationType + baseNumber

    //    FileUtility.setYarn777toTmpFolder(tmpCsvOutput)
    FileUtils.deleteDirectory(new File(tmpCsvOutput))

    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>
      val piva1 = mapKeys(keyPiva1)
      val piva2 = mapKeys(keyPiva2)
      val mainPivaValue = mapKeys(mainPiva)
      val countCsv = mapKeys(counterCsv)
      val path = tmpCsvOutput + getCsvOutputPath(baseName, piva1, piva2, daterun, countCsv)

      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      FileUtility.writeCsv(path, Option(header), records, appendMode = true)
      (mainPivaValue, path)
    }).groupByKey()
      .mapValues(values => values.head)
  }

  def writeZip(rddCsvPath: RDD[(String, String)]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val maxDimensionZip = getMaxSizeThresholdZip.toLong
    val executionId = getExecutionId
    val timestampToRun = Timestamp.valueOf(daterun)
    val baseName = getPublicationType + baseNumber

    val rddInfo = rddCsvPath.flatMap({ case (pivaHead, path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(pivaHead, daterun)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val exist = if (outputFolderIfExists.exists()) {

        var zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))

        pathInputFileCsv.getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".csv")).foreach { csvFile =>
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

    //    (executionId, operationNameVal, baseNameVal, outputFolder.getPath + zipName.replace("_1.zip", s"_$num.zip"), timestampRun, timestampRun.getTime)

    rddInfo
  }

  def putIntoZip(zip: ZipOutputStream, name: File): Unit = {
    zip.putNextEntry(new ZipEntry(name.getName))
    val in = new BufferedInputStream(new FileInputStream(name.getPath))
    var b = in.read()
    while (b > -1) {
      zip.write(b)
      b = in.read()
    }
    in.close()
    zip.closeEntry()
  }

  def getCsvOutputPath(baseName: String, piva1: String, piva2: String, date: LocalDateTime, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva1/$year/$month/${piva1}_${piva2}_${operationName}_${year}_${timestamp}_${counterCsv}.csv"
  }

  def getZipOutputName(pivaFolder: String, today: LocalDateTime): String = {
    val year = today.getYear
    //val month = ("0" + today.getMonthValue.toString).takeRight(2)
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //    val zipName = s"/${baseName}_$pivaFolder/$year/$month/${pivaFolder}_${operationName}_${year}_${timestamp}_1.zip"
    val zipName = s"/${pivaFolder}_${operationName}_${year}_${timestamp}_1.zip"
    zipName
  }

  def writeInfoInTable(rdd: RDD[(String, String, String, String, Timestamp, Long)]): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    rdd
      .toDF(InfoOutputSchema.getValues: _*)
      .repartition(10)
      .write.partitionBy(InfoOutputSchema.partition_date).mode(SaveMode.Append).parquet(getHdfsOutputBasepathInfoLog)
  }

  def getCsvOutputModel(aggFilter: DataFrame, columns: List[String]): RDD[(ListMap[String, String], Row)] = {

    distribution(aggFilter)
      .selectExpr(
        columns: _*
      )
      .rdd
      .map(row => {

        val listMap =
          ListMap(
            keyPiva1 -> row.getAs[String](keyPiva1),
            keyPiva2 -> row.getAs[String](keyPiva2),
            counterCsv -> row.getAs[String](counterCsv)
          )

        (
          listMap,
          row
        )
      })
  }

  def distribution(df: DataFrame): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get

    val window = Window.partitionBy(keyPiva1, keyPiva2)

    df
      .withColumn(counterCsv, count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col(counterCsv) / csvMaxNumberRow)) + 1).cast(StringType))
  }

  def getCsvOutputPath(baseName: String, piva1: String, piva2: String, annomese: String, sessionName: String, date: LocalDateTime, counterCsv: String): String = throw new Exception("not supported")

  def convertStringTimestampToLocalDateTime(localDateTime: String): LocalDateTime = {
    Timestamp.valueOf(localDateTime).toLocalDateTime
  }
}
