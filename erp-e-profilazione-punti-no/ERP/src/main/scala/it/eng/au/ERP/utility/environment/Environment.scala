package it.eng.au.ERP.utility.environment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream}
import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.util.Try


class Environment(appName: String, propertiesPath: String, isLocal: Boolean) {

  protected val sparkSession = if (isLocal) {
    val spark = SparkSession.builder
      .appName("Test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.eventLog.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "2")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  } else {
    SparkSession.builder
      .appName(appName)
      // allow to overwrite only the partition not the full table
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .config("spark.driver.maxResultSize", "8g")
      .config("spark.shuffle.file.buffer", "1m")
      .config("spark.shuffle.unsafe.file.output.buffer", "1m")
      .config("spark.io.compression.lz4.blockSize", "512k")
      .config("spark.sql.shuffle.partitions", "512")
      .config("spark.shuffle.registration.timeout", "120000ms")
      .config("spark.file.transferTo", "false")
      /* per evitare errori AnalysisException: Cannot overwrite a path that is also being read from.;
      dovuti dall operazione di aggiornamento della tabella data_calcolo*/
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .enableHiveSupport()
      .getOrCreate()
  }

  protected val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  protected val prop = new Properties()
  if (isLocal)
    prop.load(new FileInputStream(new File(propertiesPath)))
  else
    prop.load(fs.open(new Path(propertiesPath)).getWrappedStream)
}


object Environment {
  val executionId: Long = System.currentTimeMillis()

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

  def getOptionalProperty(key: String): Option[String] =
    Option(property.prop.getProperty(key)).map(_.trim).filter(_.nonEmpty)


  def getMandatoryAnnoMese(key: String): Option[String] = {
    val value = getOptionalProperty(key).getOrElse {
      throw new IllegalArgumentException(s"Missing mandatory property: $key")
    }

    val formatter = DateTimeFormatter.ofPattern("yyyyMM")
    // Validation: must parse correctly
    val valid = Try(LocalDate.parse(value + "01", DateTimeFormatter.ofPattern("yyyyMMdd"))).isSuccess

    if (!valid) {
      throw new IllegalArgumentException(s"Invalid format for $key: $value. Expected yyyyMM (e.g. 202507).")
    }

    Option(value)
  }

  def getSpark: SparkSession = property.sparkSession

  def getFs: FileSystem = property.fs

  val getPartitionDate: Date = new Date(executionId)

  //session
  def getSession: String = getProperty("session")

}