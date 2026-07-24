package it.eng.au.indennizziMisureGasCommon.utility.environment

import it.eng.au.indennizziMisureGasCommon.utility.kryo.CigKryoRegistrator
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties

class Environment(appName: String, loggerName: String, propertiesPath: String, needsKryo: Boolean, isLocal: Boolean) {
  private val sparkSession = if (isLocal)
    SparkSession.builder
      .appName("Test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  else {
    if (needsKryo)
      SparkSession.builder
        .appName(appName)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "1g")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.kryo.registrator", classOf[CigKryoRegistrator].getName)
        //.config("spark.kryo.classesToRegister", Array(classOf[Flow], classOf[FlowWithInfo], classOf[Consumption],
        //  classOf[ExternalDailyInfo], classOf[RcuGasMassivoP], classOf[RcuGasConnessioniDistr2], classOf[RcuGasSuspendedPdr],
        //  classOf[RcuGasTech], classOf[RcuGasVarPrelAnnuoP], classOf[RcuGasVarProfiloP], classOf[Bill2], classOf[Conn2]
        //  //, classOf[PdrInfo], classOf[PdrInfoSegment]
        //)
        //  .map(_.getName).mkString(","))
        .enableHiveSupport()
        .getOrCreate()
    else
      SparkSession.builder
      .appName(appName)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()
  }

  private val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  private val properties = new Properties()
  if (isLocal)
    properties.load(new FileInputStream(new File(propertiesPath)))
  else
    properties.load(fs.open(new Path(propertiesPath)).getWrappedStream)

  private val applicationName: String = appName
  private val logName: String = loggerName
  private val isLocalMode: Boolean = isLocal
}

object Environment {
  val startDateTime: LocalDateTime = LocalDateTime.now()
  var executionId: Long = Timestamp.valueOf(startDateTime).getTime

  private var environment: Environment = _

  def isLocal: Boolean = environment.isLocalMode

  def getOrCreate(appName: String, loggerName: String, path: String, needsKryo: Boolean = false, isLocal: Boolean = false): Unit = environment = new Environment(appName, loggerName, path, needsKryo, isLocal)

  def getProperty(key: String): String = environment.properties.getProperty(key.trim)
  def printProperties: String = environment.properties.toString.replace("{", "").replace("}", "").replace(" ", "").split(",").mkString("\n")
  def setProperty(key: String, value: String): Unit = environment.properties.setProperty(key, value)

  def spark: SparkSession = environment.sparkSession
  def sparkContext: SparkContext = environment.sparkSession.sparkContext
  def sqlContext: SQLContext = environment.sparkSession.sqlContext
  def fs: FileSystem = environment.fs

  def applicationName: String = environment.applicationName
  def logName: String = environment.logName
}