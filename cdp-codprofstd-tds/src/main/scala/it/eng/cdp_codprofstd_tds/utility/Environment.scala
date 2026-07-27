package it.eng.cdp_codprofstd_tds.utility

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

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
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
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

  //rcugas massivo executionid
  def getRcugasMassivoExecutionId: String = getProperty("rcugas.massivo.executionid")

  // settlegas tables
  def getSettleGasTdsParquetPath: String = getProperty("settleGas.gasTds.parquetPath")

  //istatRegClima tables
  def getPrtIstatRegClimaTableName: String = getProperty("prt.istatRegClima.tableName")

  //rcugas tables
  def getRcugasMassivoTableName: String = getProperty("rcugas.rcugasMassivo.tableName")
  def getRcugasConnessioniDistr2TableName: String = getProperty("rcugas.connessioniDistr2.tableName")
  def getRcugasBilanciamentoTableName: String = getProperty("rcugas.bilanciamento.tableName")
  def getPrtVsgTableName: String = getProperty("prtRcugas.prtVsg.tableName")
  def getPrtVtgTableName: String = getProperty("switchRcugas.prtVtg.tableName")
  def getPrtVsgAggRcuTableName: String = getProperty("prtRcugas.prtVsgAggRcu.tableName")
  def getPrtVtgAggRcuTableName: String = getProperty("switchRcugas.prtVtgAggRcu.tableName")


  //hdfs
  def getCodProfStdDaTds: String = getProperty("hdfs.output.cod_prof_std_da_tds")
  def getCodPrfStdDaTdsTableName: String = getProperty("cod_prof_std_da_tds.tableName")

  //exclusion filter
  def isExclusionPdrFilterActive: String = getProperty("exclusion.pdr.filter.isActive")
  def getExclusionPdrFilterCsvPath: String = getProperty("exclusion.pdr.filter.csv.path")

  //forzatura filter
  def isFilterPdrForzaturaActive: String = getProperty("filterPdr.forzatura.isActive")
  def getFilterPdrForzaturaPath: String = getProperty("filterPdr.forzatura.path")

  //date to run
  def getStartDataRicezione: String = getProperty("start.data.ricezione")
  def getEndDataRicezione: String = getProperty("end.data.ricezione")
  def getFreezeDate: String = getProperty("freeze.date")
  def getAnnoCompetenza: String = getProperty("anno.competenza")


}
