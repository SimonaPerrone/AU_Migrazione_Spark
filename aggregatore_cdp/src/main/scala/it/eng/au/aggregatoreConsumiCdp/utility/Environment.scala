package it.eng.au.aggregatoreConsumiCdp.utility

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.joda.time.{Instant, LocalDateTime}

import java.io.{File, FileInputStream}
import java.util.Properties

class Environment(appName: String, propertiesPath: String, isLocalMode: Boolean) {
  private val isLocal: Boolean = isLocalMode

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
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
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
  val executionId: Long = new Instant().getMillis
  val dateRun: String = new LocalDateTime(executionId).toString("yyyy-MM-dd HH:mm:ss")
  val dateCurrentThermalYear: String = DateTimeUtility.getCurrentThermalYear(Environment.dateRun)

  private var cdpEnvironment: Environment = _

  def isLocalMode: Boolean = cdpEnvironment.isLocal

  def getOrCreate(appName: String, path: String, isLocal: Boolean = false): Unit = cdpEnvironment = new Environment(appName, path, isLocal)

  def getProperty(key: String): String = cdpEnvironment.prop.getProperty(key)
  def setProperty(key: String, value: String): Unit = cdpEnvironment.prop.setProperty(key, value)
  def getSparkProperty(key: String): Option[String] = cdpEnvironment.sparkSession.sparkContext.getConf.getOption(key)
  def printProperties: String = cdpEnvironment.prop.toString.replace("{", "").replace("}", "").replace(" ", "").split(",").mkString("\n")

  def spark: SparkSession = cdpEnvironment.sparkSession
  def fs: FileSystem = cdpEnvironment.fs
  def sqlContext: SQLContext = spark.sqlContext
  def sparkContext: SparkContext = spark.sparkContext

  def getOutputFileCouples: String = getProperty("output.file.couples")
  def getIsilonBasepathTmp: String = getProperty("isilon.basepath.tmp")
  def getIsilonBasepathOut: String = getProperty("isilon.basepath.out")
  def getHDFSCsvOutputPath: String = getProperty("hdfs.output.basepath.csv")
  def getHDFSCsvExportOutputPath: String = getProperty("hdfs.output.basepath.csv.export")
  def getHDFSInfoLogPath: String = getProperty("hdfs.output.basepath.infoLog")
  def getCdpDatiPrelievoGasTableName: String = getProperty("cdpDatiPrelievoGas.tableName")
  def getAggregatoreInfoLogTableName: String = getProperty("aggregatoreInfoLogCdp.tableName")

  def getMaxSizeThresholdZip: String = getProperty("maxSizeThresholdZip")
  def getCsvMaxRowLength: String = getProperty("csvMaxRowLength")
  def getCsvFileSessionName: String = getProperty("csvFileSessionName")

  def getCodProfTableName: String = getProperty("cdp.caFinalCodProf.tableName")
  def getCodProfExecutionId: String = getProperty("cdp.caFinalCodProf.executionid")
  def getCaFinalTableName: String = getProperty("cdp.caFinal.tableName")
  def getCaFinalExecutionId: String = getProperty("cdp.caFinal.executionid")
  def getCaTableName: String = getProperty("ca.tableName")
  def getCaPreFinalTableName: String = getProperty("caPreFinal.tableName")
  def getRcugasMassivoFreezeTableName: String = getProperty("rcugas.massivofreeze.tableName")
  def getRcuAziendaTableName: String = getProperty("rcu.azienda.tableName")
  def getRcugasDistributoreTableName: String = getProperty("rcugas.dist.tableName")
  def getRcugasUdbTableName: String = getProperty("rcugas.udb.tableName")
  def getValidatedFlowTableName: String = getProperty("cpd.validatedFlows.tableName")
}