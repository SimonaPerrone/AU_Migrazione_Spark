package it.au.misure.ingestionMisureGasUnico.utility.environment

import java.sql.Date
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class Environment(appName: String, isLocal: Boolean) {
  private val sparkSession = if (isLocal) {
    SparkSession.builder
      .appName("Test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "2")
      .enableHiveSupport()
      .getOrCreate()

  }
  else {
    SparkSession.builder
      .appName(appName)
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()
  }

  private val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

}

object Environment {
  val executionId: Long = System.currentTimeMillis()

  private var property: Environment = _

  def getOrCreate(appName: String, isLocal: Boolean = false): Unit = property = new Environment(appName, isLocal)

  def getSpark: SparkSession = property.sparkSession

  def getFs: FileSystem = property.fs

  val getPartitionDate: Date = new Date(executionId)

}
