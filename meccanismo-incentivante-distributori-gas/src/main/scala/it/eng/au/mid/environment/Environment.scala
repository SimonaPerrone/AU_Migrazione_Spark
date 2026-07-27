package it.eng.au.mid.environment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream}
import java.time.LocalDate
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

  val executionId: Long = System.currentTimeMillis()
  val processDate: LocalDate = LocalDate.now()

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

  def getFs: FileSystem = property.fs

}