package it.eng.au.scambioDatiGasivori.controller.traits

import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.InfoOutputSchema
import it.eng.au.scambioDatiGasivori.utility.{Environment, Properties}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ceil, col, count, monotonically_increasing_id}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.io.{BufferedInputStream, File, FileInputStream}
import java.sql.Timestamp
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.immutable.ListMap

trait RunnableAggregatorTrait extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def getTmpCsvOutput: String = Properties.getIsilonBasepathTmp + s"/tmp/$getPublicationType/" + getPublicationType + baseNumber + "/" + operationName
  def getPathZipOutput: String = Properties.getIsilonBasepathOut + s"/$getPublicationType/" + getPublicationType + baseNumber
  def getCsvMaxRowLength: Some[Long] = Some(Properties.getMaxNumRowFile.toLong)
  def getDateToRun: String = Properties.getDateRun
  def getPublicationType: String = Properties.getPublicationType
  def getInputTableFiltering: String
  def getYearMonth: String = Properties.getYearMonth
  def getMaxSizeThresholdZip: String = Properties.getMaxSizeThresholdZip
  def getHdfsOutputBasepathInfoLog: String = Properties.getInfoLogBasepath

  def convertColumnsToString(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((current, c) => current.withColumn(c, col(c).cast(StringType)))
  }

  val CSV_SEPARATOR = ";"

  val counterCsv: String = "counterCsv"
  val aggregatoColumns: ListMap[String, String]
  val baseNumber: String
  val operationName: String
  val destName: String
  val keyField: String
  val csvFields: List[String]
  val writeCsvHeader: Boolean = true
  val isAmmissibilita: Boolean

  def inputDao: Dao

  def run(): Unit = {
    val rdd = getRdd()
    writeFiles(rdd)
  }

  def getRdd(): RDD[(Map[String, String], Row)] = {
    val aggregato = getAggregato().filter(col(keyField).isNotNull)
    val aggregatoForCsv = convertColumnsToString(aggregato).na.fill("")
    getDistributedRDD(aggregatoForCsv, csvFields)
  }

  def writeFiles(rdd: RDD[(Map[String, String], Row)]): Unit

  def getAggregato(): DataFrame

  def getDistributedRDD(df: DataFrame, csvFields: List[String]): RDD[(Map[String, String], Row)]

  def writeInfoInTable(rdd: RDD[(String, String, String, String, Timestamp, Long)]): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    rdd
      .toDF(InfoOutputSchema.getValues: _*)
      .repartition(10)
      .write.partitionBy(InfoOutputSchema.partition_date).mode(SaveMode.Append).parquet(getHdfsOutputBasepathInfoLog)

    if (!Environment.isLocal) Environment.spark.sql(s"MSCK REPAIR TABLE ${Properties.getInfoLogTableName}")
  }

  def distribution(df: DataFrame): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get

    val window = Window.partitionBy(keyField)
    df
      .withColumn(counterCsv, count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col(counterCsv) / csvMaxNumberRow)) + 1).cast(StringType))
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
}
