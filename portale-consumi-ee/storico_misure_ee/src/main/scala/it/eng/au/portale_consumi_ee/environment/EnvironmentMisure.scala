package it.eng.au.portale_consumi_ee.environment

import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream}
import java.sql.Date
import java.util.Properties


class EnvironmentMisure(appName: String, propertiesPath: String, isLocal: Boolean,storic:Boolean) {
  protected val sparkSession = if (isLocal) {
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
  } else if ( storic ) {
    SparkSession.builder
      .appName(appName)
      .config("spark.shuffle.service.enabled", "false")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.rdd.compress", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.sql.parquet.compression.codec", "uncompressed")
      .config("spark.sql.parquet.binaryAsString", "true")
      .config("hive.exec.dynamic.partition", "true")
      //write opertation on hive affectiong only partition specified storic operation
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      // force read data types from hive metastore
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.hive.metastorePartitionPruning", "true")
      .config("spark.sql.parquet.enableVectorizedReader","false")
      .enableHiveSupport()
      .getOrCreate()
  } else { // 3M case
    SparkSession.builder
      .appName(appName)
      .config("spark.shuffle.service.enabled", "false")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.rdd.compress", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.sql.parquet.compression.codec", "uncompressed")
      .config("spark.sql.parquet.binaryAsString", "true")
      .config("hive.exec.dynamic.partition", "true")
      //write opertation on hive affectiong only partition specified storic operation
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      // force read data types from hive metastore
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.hive.metastorePartitionPruning", "true")
      //        .config("spark.mongodbs.input.uri", s"${mongouri}")
      //        .config("spark.mongodbs.output.uri", s"${mongouri}")
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

object EnvironmentMisure {
  val executionId: Long = System.currentTimeMillis()

  private var property: EnvironmentMisure = _

  def getOrCreate(appName: String, path: String, isLocal: Boolean = false,storic:Boolean): Unit =
    property = new EnvironmentMisure(appName, path, isLocal,storic)

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

  val getPartitionDate: Date = new Date(executionId)

  //session
  def getSession: String = getProperty("session")

}

