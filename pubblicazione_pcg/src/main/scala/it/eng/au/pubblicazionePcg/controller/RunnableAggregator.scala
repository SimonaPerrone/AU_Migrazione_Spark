package it.eng.au.pubblicazionePcg.controller

import it.eng.au.pubblicazionePcg.Driver.logger
import it.eng.au.pubblicazionePcg.controller.PubblicazionePCG.importMisure
import it.eng.au.pubblicazionePcg.schema.{InfoOutputSchema, OutputCsvSchema}
import it.eng.au.pubblicazionePcg.utility.Constants.CSV_SEPARATOR
import it.eng.au.pubblicazionePcg.utility.DateTimeUtility.convertStringTimestampToLocalDateTime
import it.eng.au.pubblicazionePcg.utility.FileUtility
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.immutable.ListMap

trait RunnableAggregator extends Serializable {

  val separator = ";"
  val counterCsv: String = "counterCsv"
  val key: String
  val baseName: String
  val writeCsvHeader: Boolean = true

  def getNumLinesPerCsv(implicit prop: Properties): Int = prop.getProperty("num.linesPerCsv").toInt

  def getDateToRun(implicit prop: Properties): String = prop.getProperty("date.run")

  def getAnnoMese(implicit prop: Properties): String = prop.getProperty("year.month")

  def getHdfsOutputBasepathInfoLog(implicit prop: Properties): String = prop.getProperty("hdfs.output.basepath.infoLog")

  def getCsvOutput(implicit prop: Properties): String = prop.getProperty("isilon.basepath.out") + "/CLG/" + baseName

  def prepareDataFrame(df: DataFrame)(implicit prop: Properties, sqlContext: SQLContext): RDD[(ListMap[String, String], Row)]

  def run(df: DataFrame)(implicit prop: Properties, fs: FileSystem, sqlContext: SQLContext, sc: SparkContext): Unit = {
    val sbgMisure = importMisure(df)
    val dfReadyForCsv = prepareDataFrame(sbgMisure)
    val rddInfo = writeCsv(dfReadyForCsv)
    writeInfoIntoTable(rddInfo)
  }


  def writeCsv(sbgMisure: RDD[(ListMap[String, String], Row)])(implicit prop: Properties, sc: SparkContext, sqlContext: SQLContext): RDD[(String, String, String, Timestamp, Long)] = {

    val csvOutput = getCsvOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val timestamp = Timestamp.valueOf(daterun)
    val annoMese = getAnnoMese

    val rddInfo = sbgMisure
      .groupByKey
      .map({
        case (mapKeys, rows) =>
          val piva = mapKeys(key)
          val countCsv = mapKeys(counterCsv)
          val path = csvOutput + getCsvOutputPath(baseName, piva, annoMese, daterun, countCsv)
          val header = OutputCsvSchema.header

          val records = rows.toList.map(row => {
            OutputCsvSchema.getValues.map(column => {
              row.getAs[String](column)
            }).mkString(CSV_SEPARATOR)
          })

          val outputFile = new File(path)
          val exists = if (outputFile.getParentFile.exists()) {
            FileUtility.writeCsv(path, header, records, appendMode = true)
            ""
          }
          else {
            logger.warn(s"Couldn't write to ${outputFile.getParentFile}, the path does not exits.")
            s"Couldn't write to ${outputFile.getParentFile}, the path does not exits."
          }

          (annoMese, baseName, if (exists == "") outputFile.getPath else exists, timestamp, timestamp.getTime)
      })

    rddInfo
  }

  def getCsvOutputPath(baseName: String, piva: String, annomese: String, daterun: LocalDateTime, counterCsv: String)(implicit prop: Properties): String = {
    val year = daterun.getYear.toString
    val month = f"${daterun.getMonthValue}%02d" //needed to add padding 0 if the month is a 1-digit number
    val timestamp = daterun.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    s"/${baseName}_$piva/$year/$month/${piva}_PCG_${annomese}_${timestamp}_${counterCsv}.csv"
  }

  def writeInfoIntoTable(rdd: RDD[(String, String, String, Timestamp, Long)])(implicit prop: Properties, sc: SparkContext, fs: FileSystem, sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    rdd
      .toDF(InfoOutputSchema.getValues: _*)
      .repartition(10)
      .write
      .partitionBy(InfoOutputSchema.partition_date).mode(SaveMode.Append).parquet(getHdfsOutputBasepathInfoLog)
  }
}
