package it.eng.au.freezerPreCalcolo.utility.environment

import java.io.{File, FileInputStream}
import java.util.Properties
import java.sql.Date
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

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
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
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
  def printProperties: String = property.prop.toString.replace("{", "").replace("}", "").replace(" ", "").split(",").mkString("\n")

  def setProperty(key: String, value: String): Unit = property.prop.setProperty(key, value)

  def getSpark: SparkSession = property.sparkSession

  def getFs: FileSystem = property.fs

  val getPartitionDate: Date = new Date(executionId)

  //freezeDate and session
  def getFreezeDate: String = getProperty("freeze_date")
  def getSession: String = getProperty("session")

  //input table
  def getInputTableFreezer: String = getProperty("input.table.freezer")

  //parquetPath
  def getAggRcuGasMassivo: String = getProperty("agg.rcuGasMassivo.parquetPath")
  def getRcuGasConnessioniDistr2: String = getProperty("agg.rcuGasConnessioniDistr2.parquetPath")
  def getRcuGasVarMisurratore: String = getProperty("agg.rcuGasVarMisurratore.parquetPath")
  def getRcuGasVarConvertitore: String = getProperty("agg.rcuGasVarConvertitore.parquetPath")

  //hdfs
  def getRcugasMassivoFreeze: String = getProperty("hdfs.output.rcugas_massivo_freeze")
  def getRcugasTechFreeze: String = getProperty("hdfs.output.rcugas_tech_freeze")

  //hive
  def getRcugasMassivoTableName: String = getProperty("rcugasMassivo.tableName")
  def getRcugasTechTableName: String = getProperty("rcugasTech.tableName")

}
