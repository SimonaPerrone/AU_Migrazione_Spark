package it.eng.au.cceCalcolo.utility.environment

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * Inizializza la SparkSession e gli altri punti di accesso necessari al processo (quali fs) e legge le properties fornite come input
 * @param appName nome dell'applicazione
 * @param loggerName nome del log dell'applicazione
 * @param isLocal true se il processo è eseguito in locale
 */
class Environment(appName: String, loggerName: String, isLocal: Boolean) {
  private val sparkSession = if (isLocal)
    SparkSession.builder
      .appName("Test")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  else {
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

  private val applicationName: String = appName
  private val logName: String = loggerName
  private val isLocalMode: Boolean = isLocal
}

object Environment {
  val startDateTime: LocalDateTime = LocalDateTime.now()
  val executionId: Long = Timestamp.valueOf(startDateTime).getTime

  private var environment: Environment = _

  def isLocal: Boolean = environment.isLocalMode

  def getOrCreate(appName: String, loggerName: String, isLocal: Boolean = false): Unit = environment = new Environment(appName, loggerName, isLocal)

  def spark: SparkSession = environment.sparkSession
  def sparkContext: SparkContext = environment.sparkSession.sparkContext
  def sqlContext: SQLContext = environment.sparkSession.sqlContext

  def applicationName: String = environment.applicationName
  def logName: String = environment.logName

}