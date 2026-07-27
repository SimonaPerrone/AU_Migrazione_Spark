package it.eng.au.ccgPubblicazione.utility

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

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
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private var property: Environment = _

  def isLocalMode: Boolean = property.isLocal

  def getOrCreate(appName: String, path: String, isLocal: Boolean = false): Unit = property = new Environment(appName, path, isLocal)

  def getProperty(key: String): String = property.prop.getProperty(key)
  def printProperties: String = property.prop.toString.replace("{","").replace("}","").replace(" ","").split(",").mkString("\n")
  def setProperty(key: String, value: String): Unit = property.prop.setProperty(key, value)

  def spark: SparkSession = property.sparkSession
  def fs: FileSystem = property.fs
  def sqlContext: SQLContext = spark.sqlContext
  def sparkContext: SparkContext = spark.sparkContext

  def getDateRun: String = getProperty("daterun")

  def getIsilonBasepathTmp: String = getProperty("isilon.basepath.tmp")
  def getIsilonBasepathOut: String = getProperty("isilon.basepath.out")

  def getMaxDimensionZipFileByte: String = getProperty("maxDimensionZipFileByte")
  def getMaxNumRowFile: String = getProperty("maxNumRowFile")

  def getAggConsumptionTableName: String = getProperty("agg.consumption.tableName")
  def getAggIncoerentiConsumptionTableName: String = getProperty("agg.consumption.incoerenti.tableName")
  def getAggEsclusiConsumptionTableName: String = getProperty("agg.consumption.esclusi.tableName")
  def getAggValidatedFlowTableName: String = getProperty("agg.validate.tableName")

  def getSbgConsumptionTableName: String = getProperty("sbg.consumption.tableName")
  def getSbgIncoerentiConsumptionTableName: String = getProperty("sbg.consumption.incoerenti.tableName")
  def getSbgEsclusiConsumptionTableName: String = getProperty("sbg.consumption.esclusi.tableName")
  def getSbgValidatedFlowTableName: String = getProperty("sbg.validate.tableName")

  def getCdpConsumptionTableName: String = getProperty("cdp.consumption.tableName")
  def getCdpValidatedFlowTableName: String = getProperty("cdp.validate.tableName")

  def getConsumptionExecutionid: String = getProperty("consumption.executionid")
  def setConsumptionExecutionid(partition: String): Unit = setProperty("consumption.executionid", partition)

  def getDataRichiesta: String = getProperty("datarichiesta")
  def setDataRichiesta(data_richiesta: String): Unit = setProperty("datarichiesta", data_richiesta)

  def getSessione: String = getProperty("sessione")
  def setSessione(sessione: String): Unit = setProperty("sessione", sessione)

  def getRequestPdrTableName: String = getProperty("request.pdr.tableName")
  def getRequestFilterTableName: String = getProperty("request.filter.tableName")
  def getRequestEsitoTableName: String = getProperty("request.esito.tableName")
  def getRequestEsitoParquet: String = getProperty("request.esito.parquet")
  def getRequestEsitoTableNameExport: String = getProperty("request.esito.tableName.export")
  def getRequestEsitoParquetExport: String = getProperty("request.esito.parquet.export")

  def getRecoveryRequest: String = getProperty("recovery.request")
  def getCsvPatheRcoveryRequest: String = getProperty("csv.path.recovery.request")

  def getClassGroupMeasureRangeMaxPath: String = getProperty("class.group.measure.range.max.path")
  def getRcugasPdrTableName: String = getProperty("rcugas.pdr.tableName")
  def getRcugasVarMisuratoreTableName: String = getProperty("rcugas.varMisuratore.tableName")
  def getNumberOfDaysThresholdForGdm: String = getProperty("incoerenzaGDM.numberOfDays.threshold")
}
