package it.eng.au.pubblicazione_cce.utility.environment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.Properties

class Environment(appName: String, propertiesPath: String, isLocal: Boolean) {

  private val sparkSession = if (isLocal) {
    val spark = SparkSession.builder
      .appName("Test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.eventLog.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  } else {
    SparkSession.builder
      .appName(appName)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.broadcastTimeout", "2000")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.shuffle.file.buffer", "1m")
      .config("spark.shuffle.unsafe.file.output.buffer", "1m")
      .config("spark.io.compression.lz4.blockSize", "512k")
      .config("spark.shuffle.registration.timeout", "120000ms")
      .config("spark.file.transferTo", "false")
      .enableHiveSupport()
      .getOrCreate()
  }

  private val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  private val prop = new Properties()
  if (isLocal)
    prop.load(new FileInputStream(new File(propertiesPath)))
  else
    prop.load(fs.open(new Path(propertiesPath)).getWrappedStream)
}

object Environment {

  val processTimestamp: Timestamp = Timestamp.valueOf(LocalDateTime.now())
  val fileTimestamp: String = processTimestamp.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
  val executionId: Long = System.currentTimeMillis()
  val processDate: LocalDate = processTimestamp.toLocalDateTime.toLocalDate

  private var property: Environment = _

  def getOrCreate(appName: String, path: String, isLocal: Boolean = false): Unit =
    property = new Environment(appName, path, isLocal)

  def getProperty(key: String): String = property.prop.getProperty(key)

  def printProperties: String = property
    .prop.toString
    .replace("{", "")
    .replace("}", "")
    .replace(" ", "")
    .split(",")
    .mkString("\n")

  def setProperty(key: String, value: String): Unit = property.prop.setProperty(key, value)

  def getSpark: SparkSession = property.sparkSession

  def getCceRichiestaPodTableName: String = getProperty("hive.table.cceRichiestaPod")

  def getCceRichiestaFiltroTableName: String = getProperty("hive.table.cceRichiestaFiltro")

  def getCceEsitoTableName: String = getProperty("hive.table.cceEsito")

  def getCceEsitoExportTableName: String = getProperty("hive.table.cceEsitoExport")

  def getCceCalcoloAnagraficaTableName: String = getProperty("hive.table.cceCalcoloAnagrafica")

  def getCceCalcoloPTableName: String = getProperty("hive.table.cceCalcoloP")

  def getCceCalcoloPeinTableName: String = getProperty("hive.table.cceCalcoloPein")

  def getCceCalcoloPRTableName: String = getProperty("hive.table.cceCalcoloPR")

  def getCceCalcoloPReinTableName: String = getProperty("hive.table.cceCalcoloPRein")

  def getCceCalcoloTrattamentoTableName: String = getProperty("hive.table.cceCalcoloTrattamento")

  def getCceCalcTrackTableName: String = getProperty("hive.table.cceCalcTrack")
  def getCceCalcoloCaTableName: String = getProperty("hive.table.cceCalcoloCa")
  def getCceCalcoloCaFlussiTableName: String = getProperty("hive.table.cceCalcoloCaFlussi")

  // PROPERTIES

  def getOutputFilePath: String = getProperty("output.file.path")

  def getOutputFileTemporaryPath: String = getProperty("output.file.temporary_path")

  def getOutputFileCsvMaxRow: Int = getProperty("output.file.csv_max_row").toInt

  def getOutputFileZipMaxByteSize: Long = getProperty("output.file.zip_max_byte_size").toLong

}