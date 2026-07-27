package it.eng.au.sgsFlussoStoricoGas.utility.environment

import it.eng.au.sgsFlussoStoricoGas.utility.kryo.SgsKryoRegistrator
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
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic") // in questo modo è possibile sovrascrivere le singole partizioni su Hive
      .config("hive.exec.dynamic.partition.mode", "nonstrict") // vedi sopra
      .config("spark.sql.crossJoin.enabled", "true") // abilita la cross join
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // utilizza Kryo come serializziatore per le classe registrate qui sotto
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .config("spark.kryo.registrator", classOf[SgsKryoRegistrator].getName)
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
  /** Orario di inizio del processo come timestamp. */
  val executionId: Long = Timestamp.valueOf(startDateTime).getTime

  private var property: Environment = _

  /**
   * Inizializza Spark, e crea un'istanza di [[Environment]], dal quale si può accedere a spark, sqlContext, ..., e a tutte le properties presenti nel file di configurazione
   *
   * @param appName        nome dell'applicazione
   * @param loggerName     nome del log dell'applicazione
   * @param propertiesPath percorso del file di configurazione
   * @param isLocal        true se il processo è eseguito in locale
   */
  def getOrCreate(appName: String, loggerName: String, path: String, isLocal: Boolean = false): Unit = property = new Environment(appName, loggerName, path, isLocal)

  /**
   * Restituisce il valore della configurazione [[key]]
   *
   * @param key configurazione di cui si vuole ottenere il valore
   * @return valore di [[key]] se presente
   */
  def getProperty(key: String): String = property.prop.getProperty(key.trim).trim
  def printProperties: String = property.prop.toString.replace("{", "").replace("}", "").replace(" ", "").split(",").mkString("\n")

  /**
   * Setta il valore di [[key]] a [[value]]. In questo modo, [[key]] può essere richiesta successivamente utilizzando [[Environment.getProperty]]
   */
  def setProperty(key: String, value: String): Unit = property.prop.setProperty(key, value)
  def getSpark: SparkSession = property.sparkSession
  def applicationName: String = property.applicationName
  def logName: String = property.logName

  // DB
  def getSgsDBName: String = getProperty("sgs.db")

  //rcuTable
  def getRcugasVarTrattamentoPath: String = getProperty("rcuGasVarTrattamentoP.basepath")
  def getRcugasVarMisuratorePath: String = getProperty("rcuGasVarMisuratoreP.basepath")
  def getRcugasVarConvertitiorePath: String = getProperty("rcuGasVarConvertitoreP.basepath")
  def getRcugasConnessioniDistr2Path: String = getProperty("rcuGasConnessioniDistr2P.basepath")
  def getRcugasPdrPath: String = getProperty("rcuGasPdrP.basepath")

  //prt
  def getPrtIstatRegioneClimaticaPath: String = getProperty("prtIstatRegioneClimaticaP.basepath")

  //Perimetro configs
  def getBoolPerimetroSwgS: String = getProperty("perimetroSwgS.bool")
  def getBoolPerimetroSwgA: String = getProperty("perimetroSwgA.bool")
  def getBoolPerimetroUigS: String = getProperty("perimetroUigS.bool")
  def getBoolPerimetroUigA: String = getProperty("perimetroUigA.bool")
  def getBoolPerimetroVtgS: String = getProperty("perimetroVtgS.bool")

  //Aggregazione configs
  //UDB
  def getBoolAggregazioneUdbSwgS: String = getProperty("aggregazioneUdbSwgS.bool")
  def getBoolAggregazioneUdbSwgA: String = getProperty("aggregazioneUdbSwgA.bool")
  def getBoolAggregazioneUdbUigS: String = getProperty("aggregazioneUdbUigS.bool")
  def getBoolAggregazioneUdbUigA: String = getProperty("aggregazioneUdbUigA.bool")
  def getBoolAggregazioneUdbVtgS: String = getProperty("aggregazioneUdbVtgS.bool")
  def getBoolAggregazioneUdbVtgA: String = getProperty("aggregazioneUdbVtgA.bool")
  //UDD
  def getBoolAggregazioneUddSwgS: String = getProperty("aggregazioneUddSwgS.bool")
  def getBoolAggregazioneUddSwgA: String = getProperty("aggregazioneUddSwgA.bool")
  def getBoolAggregazioneUddUigS: String = getProperty("aggregazioneUddUigS.bool")
  def getBoolAggregazioneUddUigA: String = getProperty("aggregazioneUddUigA.bool")
  def getBoolAggregazioneUddVtgS: String = getProperty("aggregazioneUddVtgS.bool")
  def getBoolAggregazioneUddVtgA: String = getProperty("aggregazioneUddVtgA.bool")

  //Pubblicazione configs
  //UDB
  def getBoolPubblicazioneUdbSwgS: String = getProperty("pubblicazioneUdbSwgS.bool")
  def getBoolPubblicazioneUdbSwgA: String = getProperty("pubblicazioneUdbSwgA.bool")
  def getBoolPubblicazioneUdbUigS: String = getProperty("pubblicazioneUdbUigS.bool")
  def getBoolPubblicazioneUdbUigA: String = getProperty("pubblicazioneUdbUigA.bool")
  def getBoolPubblicazioneUdbVtgS: String = getProperty("pubblicazioneUdbVtgS.bool")
  def getBoolPubblicazioneUdbVtgA: String = getProperty("pubblicazioneUdbVtgA.bool")
  //UDD
  def getBoolPubblicazioneUddSwgS: String = getProperty("pubblicazioneUddSwgS.bool")
  def getBoolPubblicazioneUddSwgA: String = getProperty("pubblicazioneUddSwgA.bool")
  def getBoolPubblicazioneUddUigS: String = getProperty("pubblicazioneUddUigS.bool")
  def getBoolPubblicazioneUddUigA: String = getProperty("pubblicazioneUddUigA.bool")
  def getBoolPubblicazioneUddVtgS: String = getProperty("pubblicazioneUddVtgS.bool")
  def getBoolPubblicazioneUddVtgA: String = getProperty("pubblicazioneUddVtgA.bool")


  //Sqoop tables
  def getPerimetroSwgSRawPath: String = getProperty("perimetroSwgSRaw.basepath")
  def getPerimetroSwgARawPath: String = getProperty("perimetroSwgARaw.basepath")
  def getPerimetroUigSRawPath: String = getProperty("perimetroUigSRaw.basepath")
  def getPerimetroUigARawPath: String = getProperty("perimetroUigARaw.basepath")
  def getPerimetroVtgSRawPath: String = getProperty("perimetroVtgSRaw.basepath")
  def getPerimetroSwgSRawTableName: String = getProperty("perimetroSwgSRaw.table")
  def getPerimetroSwgARawTableName: String = getProperty("perimetroSwgARaw.table")
  def getPerimetroUigSRawTableName: String = getProperty("perimetroUigSRaw.table")
  def getPerimetroUigARawTableName: String = getProperty("perimetroUigARaw.table")
  def getPerimetroVtgSRawTableName: String = getProperty("perimetroVtgSRaw.table")

  //Perimetro tables
  def getPerimetroSwgSPath: String = getProperty("perimetroSwgS.basepath")
  def getPerimetroSwgAPath: String = getProperty("perimetroSwgA.basepath")
  def getPerimetroUigSPath: String = getProperty("perimetroUigS.basepath")
  def getPerimetroUigAPath: String = getProperty("perimetroUigA.basepath")
  def getPerimetroVtgSPath: String = getProperty("perimetroVtgS.basepath")
  def getPerimetroSwgSTableName: String = getProperty("perimetroSwgS.table")
  def getPerimetroSwgATableName: String = getProperty("perimetroSwgA.table")
  def getPerimetroUigSTableName: String = getProperty("perimetroUigS.table")
  def getPerimetroUigATableName: String = getProperty("perimetroUigA.table")
  def getPerimetroVtgSTableName: String = getProperty("perimetroVtgS.table")

  //Daily Consumption Agg
  def getDailyConsumptionPath: String = getProperty("dailyConsumption.basepath")
  def getDailyConsumptionIncoerentiPath: String = getProperty("dailyConsumptionIncoerenti.basepath")
  def getDailyConsumptionEsclusiPath: String = getProperty("dailyConsumptionEsclusi.basepath")
  def getDailyConsumptionTableName: String = getProperty("dailyConsumption.table")
  def getDailyConsumptionIncoerentiTableName: String = getProperty("dailyConsumptionIncoerenti.table")
  def getDailyConsumptionEsclusiTableName: String = getProperty("dailyConsumptionEsclusi.table")

  //exec track
  def getExecutionTrackPath: String = getProperty("executionTrack.basepath")

  //AggregatoreInfoDett
  def getAggregatoreInfoDettPath: String = getProperty("aggregatoreInfoDett.basepath")
  def getAggregatoreInfoDettTableName: String = getProperty("aggregatoreInfoDett.table")

  //Pubblicazioni
  def getXsdPath: String = getProperty("xsd.basepath")
  def getOutputPathXml: String = getProperty("xmlOutput.basepath")
  def getPdrPerFile: String = getProperty("pdrPerFile.constant")
  def getPubblicazioneInfoDettPath: String = getProperty("pubblicazioneInfoDett.basepath")
  def getPubblicazioneInfoDettTableName: String = getProperty("pubblicazioneInfoDett.table")
  def getPubblicazioneInfoPath: String = getProperty("pubblicazioneInfo.basepath")
  def getPubblicazioneInfoTableName: String = getProperty("pubblicazioneInfo.table")
  def getSgsReportPath: String = getProperty("sgsReport.path")
  def getSgsReportTableName: String = getProperty("sgsReport.table")

}
