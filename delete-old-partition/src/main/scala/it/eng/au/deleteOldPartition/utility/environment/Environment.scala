package it.eng.au.deleteOldPartition.utility.environment

import it.eng.au.deleteOldPartition.utility.kryo.AggKryoRegistrator
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.joda.time.LocalDateTime

import java.io.{File, FileInputStream}
import java.sql.Date
import java.util.Properties


class Environment(appName: String, propertiesPath: String, isLocal: Boolean) {

  private val sparkSession = if (isLocal) {
    SparkSession.builder
      .appName("Test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  }
  else {
    SparkSession.builder
      .appName(appName)
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .config("spark.kryo.registrator", classOf[AggKryoRegistrator].getName)
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

  private var property: Environment = _

  def getOrCreate(appName: String, path: String, isLocal: Boolean = false): Unit = property = new Environment(appName, path, isLocal)

  def getProperty(key: String): String = property.prop.getProperty(key)
  def printProperties: String = property.prop.toString.replace("{","").replace("}","").replace(" ","").split(",").mkString("\n")

  def setProperty(key: String, value: String): Unit = property.prop.setProperty(key, value)

  def getSpark: SparkSession = property.sparkSession

  def getFs: FileSystem = property.fs

  val getPartitionDate = new LocalDateTime(executionId)

  def getTables: String = getProperty("tables.to.optimize")
  def getPathToLog: String = getProperty("path.to.log")
  def getNumPartitionToRemain: String = getProperty("num.partition.remain")

}