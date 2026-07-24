package it.eng.au.gsvAggregatoreConsumi.utility.environment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties

/**
 * Inizializza la SparkSession e gli altri punti di accesso necessari al processo (quali fs) e legge le properties fornite come input
 * @param appName nome dell'applicazione
 * @param loggerName nome del log dell'applicazione
 * @param propertiesPath percorso del file di configurazione
 * @param isLocal true se il processo è eseguito in locale
 */
class Environment(appName: String, loggerName: String, propertiesPath: String, isLocal: Boolean) {

  /** Spark Session, generata dal metodo [[Environment.getOrCreate]] */
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
      //      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")                // in questo modo è possibile sovrascrivere le singole partizioni su Hive
      .config("hive.exec.dynamic.partition.mode", "nonstrict")                      // vedi sopra
      .config("spark.sql.crossJoin.enabled", "true")                                // abilita la cross join
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()
  }

  private val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  /** Contiene le properties lette dal file di configurazione fornito come input al processo. */
  private val prop = new Properties()
  if (isLocal)
    prop.load(new FileInputStream(new File(propertiesPath)))
  else
    prop.load(fs.open(new Path(propertiesPath)).getWrappedStream)

  private val applicationName: String = appName
  private val logName: String = loggerName
}

object Environment {
  /** Orario di inizio del processo */
  val startDateTime: LocalDateTime = LocalDateTime.now()
  /** Orario di inizio del processo come timestamp, utilizzato in fase di scrittura delle tabelle di output come campo di partizionamento. */
  val executionId: Long = Timestamp.valueOf(startDateTime).getTime

  private var property: Environment = _

  /**
   * Inizializza Spark, e crea un'istanza di [[Environment]], dal quale si può accedere a spark, sqlContext, ..., e a tutte le properties presenti nel file di configurazione
   * @param appName nome dell'applicazione
   * @param loggerName nome del log dell'applicazione
   * @param propertiesPath percorso del file di configurazione
   * @param isLocal true se il processo è eseguito in locale
   */
  def getOrCreate(appName: String, loggerName: String, path: String, isLocal: Boolean = false): Unit = property = new Environment(appName, loggerName, path, isLocal)

  /**
   * Restituisce il valore della configurazione [[key]]
   * @param key configurazione di cui si vuole ottenere il valore
   * @return valore di [[key]] se presente
   */
  def getProperty(key: String): String = property.prop.getProperty(key.trim).trim
  def printProperties: String = property.prop.toString.replace("{","").replace("}","").replace(" ","").split(",").mkString("\n")

  /**
   * Setta il valore di [[key]] a [[value]]. In questo modo, [[key]] può essere richiesta successivamente utilizzando [[Environment.getProperty]]
   */
  def setProperty(key: String, value: String): Unit = property.prop.setProperty(key, value)

  def getSpark: SparkSession = property.sparkSession

  def applicationName: String = property.applicationName
  def logName: String = property.logName


  //agg tables
  def getDailyConsumptionTable: String = getProperty("dailyConsumption.table")
  def getDailyConsumptionExclTable: String = getProperty("dailyConsumptionExcl.table")
  def getDailyConsumptionIncGdMTable: String = getProperty("dailyConsumptionIncGdM.table")
  def getDailyConsumptionPath: String = getProperty("dailyConsumption.basepath")
  def getDailyConsumptionExclPath: String = getProperty("dailyConsumptionExcl.basepath")
  def getDailyConsumptionIncGdMPath: String = getProperty("dailyConsumptionIncGdM.basepath")


  //gsv tables
  def getGsvConsForniturePath: String = getProperty("gsvConsForniture.basepath")
  def getGsvConsFornitureTable: String = getProperty("gsvConsForniture.table")
  def getGsvConsRichiestaPath: String = getProperty("gsvConsRichiesta.basepath")
  def getGsvConsRichiestaTable: String = getProperty("gsvConsRichiesta.table")
  def getGsvConsAggregatoPath: String = getProperty("gsvConsAggregato.basepath")
  def getGsvConsAggregatoTable: String = getProperty("gsvConsAggregato.table")


}