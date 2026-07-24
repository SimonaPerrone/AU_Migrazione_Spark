package it.eng.au.scambioDatiGasivori.controller.traits

import it.eng.au.scambioDatiGasivori.schema.output.csv.FilieraCsvOutputSchema
import it.eng.au.scambioDatiGasivori.utility.FileUtility
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.ZipOutputStream

trait AggregatoreGasivori extends RunnableAggregatorTrait {
  override val operationName: String = "AGGREGATO"
  override val csvFields: List[String] = FilieraCsvOutputSchema.getValues
  override val isAmmissibilita: Boolean = false

  override def writeFiles(rdd: RDD[(Map[String, String], Row)]): Unit = {
    val rddCsvPath = writeCsvTmp(rdd, csvFields)
    val rddWithInfo = writeZip(rddCsvPath)
    writeInfoInTable(rddWithInfo)
  }

  override def getAggregato(): DataFrame = {
    val executionId = getInputTableFiltering
    var df = inputDao.readPartition(executionId)

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      df = df.withColumnRenamed(dailyName, fileName)
    })

    df
      .withColumn(FilieraCsvOutputSchema.DATA_INIZIO, from_unixtime(unix_timestamp(col(FilieraCsvOutputSchema.DATA_INIZIO), "yyyy-MM-dd HH:mm:ss"), "dd/MM/yyyy"))
      .withColumn(FilieraCsvOutputSchema.DATA_FINE, from_unixtime(unix_timestamp(col(FilieraCsvOutputSchema.DATA_FINE), "yyyy-MM-dd HH:mm:ss"), "dd/MM/yyyy"))
      .selectExpr(aggregatoColumns.values.toList: _*)
  }

  def writeCsvTmp(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String]): RDD[(String, String)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val publicationType = getPublicationType
    val baseName = publicationType + baseNumber
    val daterun = Timestamp.valueOf(getDateToRun).toLocalDateTime
    val yearMonth = getYearMonth

    FileUtils.deleteDirectory(new File(tmpCsvOutput))

    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>
      val countCsv = mapKeys(counterCsv)
      val path = tmpCsvOutput + getCsvOutputPath(baseName, mapKeys, daterun, countCsv, yearMonth)

      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      val header = if (writeCsvHeader)
        Some(columnsField.mkString(CSV_SEPARATOR))
      else None

      FileUtility.writeCsv(path, header, records, appendMode = true)
      (mapKeys(keyField), path)
    }).groupByKey()
      .mapValues(values => values.head)
  }

  def writeZip(rddCsvPath: RDD[(String, String)]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = Timestamp.valueOf(getDateToRun).toLocalDateTime
    val maxDimensionZip = getMaxSizeThresholdZip.toLong
    val filteringValue = getInputTableFiltering
    val timestampToRun = Timestamp.valueOf(getDateToRun)
    val yearMonth = getYearMonth
    val timestamp = daterun.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    val rddInfo = rddCsvPath.flatMap({ case (pivaHead, path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(timestamp, yearMonth, 1)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val exist = if (outputFolderIfExists.exists() && outputFolderIfExists.canWrite) {

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
            zipName = getZipOutputName(timestamp, yearMonth, count)
            zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
            putIntoZip(zip, csvFile)
          }
        }
        zip.close()
        ""
      } else {
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits.")
        s" Couldn't write to ${outputFolder + originalZipName}, the path does not exits or permission are not set properly."
      }
      val result = (1 to count).toList.map { num =>
        (filteringValue, operationName, destName,
          if (exist == "") outputFolder + getZipOutputName(timestamp, yearMonth, 1)
          else exist
          , timestampToRun, timestampToRun.getTime)
      }
      result
    })

    rddInfo
  }

  def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, counterCsv: String, annoMese: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyField)

    s"/${baseName}_$pivaPathFolderHead/$year/$month/FORNITURE_GASIVORI_${annoMese}_${counterCsv}_${timestamp}.csv"
  }

  def getZipOutputName(timestamp: String, yearMonth: String, count: Int): String = {
    val zipName = s"/FORNITURE_GASIVORI_${yearMonth}_${count.toString}_${timestamp}.zip"
    zipName
  }

  def getDistributedRDD(df: DataFrame, csvFields: List[String]): RDD[(Map[String, String], Row)] = {
    val keys = List(keyField) :+ counterCsv

    val columns = (csvFields :+ counterCsv :+ keyField).distinct

    var dfDistribution = distribution(df)
      .selectExpr(columns: _*)

    columns.foreach(column =>
      dfDistribution = dfDistribution.withColumn(column, col(column).cast(StringType))
    )

    dfDistribution
      .rdd
      .map(row => {
        val listMap = keys.map(column => column -> row.getAs[String](column)).toMap
        (listMap, row)
      })
  }
}
