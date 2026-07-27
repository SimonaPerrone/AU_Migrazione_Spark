package it.eng.au.aggregatoreConsumiCommon.utility

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.io.{File, FileInputStream}
import java.util.Properties

/**
 * Inizializza la SparkSession e gli altri punti di accesso necessari al processo (quali fs) e legge le properties fornite come input
 * @param appName nome dell'applicazione
 * @param propertiesPath percorso del file di configurazione
 * @param isLocalMode true se il processo è eseguito in locale
 */
class Environment(appName: String, propertiesPath: String, isLocalMode: Boolean) {
  private val isLocal: Boolean = isLocalMode

  /** Spark Session, generata dal metodo [[Environment.getOrCreate]] */
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
      .config("spark.dynamicAllocation.enabled", "false")                             // disattiva l'allocazione dinamica (in realtà si potrebbe rimuovere questa riga)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")                  // in questo modo è possibile sovrascrivere le singole partizioni su Hive
      .config("hive.exec.dynamic.partition.mode", "nonstrict")                        // vedi sopra
      .config("spark.sql.crossJoin.enabled", "true")                                  // abilita la cross join
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
}

object Environment {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private var aggEnvironment: Environment = _

  def isLocalMode: Boolean = aggEnvironment.isLocal

  /**
   * Inizializza Spark, e crea un'istanza di [[Environment]], dal quale si può accedere a spark, sqlContext, ..., e a tutte le properties presenti nel file di configurazione
   * @param appName nome dell'applicazione
   * @param path percorso del file di configurazione
   * @param isLocal true se il processo è eseguito in locale
   */
  def getOrCreate(appName: String, path: String, isLocal: Boolean = false): Unit = aggEnvironment = new Environment(appName, path, isLocal)

  /**
   * Restituisce il valore della configurazione [[key]]
   * @param key configurazione di cui si vuole ottenere il valore
   * @return valore di [[key]] se presente
   */
  def getProperty(key: String): String = aggEnvironment.prop.getProperty(key.trim).trim
  /** Setta il valore di [[key]] a [[value]]. In questo modo, [[key]] può essere richiesta successivamente utilizzando [[Environment.getProperty]] */
  def setProperty(key: String, value: String): Unit = aggEnvironment.prop.setProperty(key, value)
  def getSparkProperty(key: String): Option[String] = aggEnvironment.sparkSession.sparkContext.getConf.getOption(key)
  def printProperties: String = aggEnvironment.prop.toString.replace("{", "").replace("}", "").replace(" ", "").split(",").mkString("\n")

  def spark: SparkSession = aggEnvironment.sparkSession
  def fs: FileSystem = aggEnvironment.fs
  def sqlContext: SQLContext = spark.sqlContext
  def sparkContext: SparkContext = spark.sparkContext

  // data di lancio del processo
  def getDateRun: String = getProperty("daterun")
  // valore di anno (AGG) o anno-mese (SBG) da inserire nel nome del file ZIP
  def getYear: String = getProperty("year")

  // Pubblicazioni da eseguire
  def getOutputFileCouples: String = getProperty("output.file.couples")

  // Percorsi su file system e HDFS
  def getIsilonBasepathTmp: String = getProperty("isilon.basepath.tmp")
  def getIsilonBasepathOut: String = getProperty("isilon.basepath.out")
  def getHDFSTmpBasepath: String = getProperty("hdfs.tmp.basepath")
  def getHDFSOutputBasepath: String = getProperty("hdfs.output.basepath")
  def getInfoLogTableName: String = getProperty("hdfs.infoLog.tableName")

  // Tabelle in scrittura
  def getAggregatoTableName: String = getProperty("aggregato.tableName")
  def getEsclusiTableName: String = getProperty("esclusi.tableName")
  def getElencoFlussiDettaglioEsclusiTableName: String = getProperty("elencoFlussiDettaglioEsclusi.tableName")
  def getIncoerentiTableName: String = getProperty("incoerenti.tableName")
  def getIncoerentiDettaglioTableName: String = getProperty("incoerentiDettaglio.tableName")
  def getSospesiTableName: String = getProperty("sospesi.tableName")
  def getPdrDettaglioUnicoTableName: String = getProperty("pdrDettaglioUnico.tableName")
  def getElencoFlussiDettaglioUnicoTableName: String = getProperty("elencoFlussiDettaglioUnico.tableName")
  def getPdrDettaglioIncoerentiTableName: String = getProperty("pdrDettaglioIncoerenti.tableName")
  def getElencoFlussiDettaglioIncoerentiTableName: String = getProperty("elencoFlussiDettaglioIncoerenti.tableName")
  def getPdrDettaglioDeltaNegativoTableName: String = getProperty("pdrDettaglioDeltaNegativo.tableName")
  def getElencoFlussiDettaglioDeltaNegativoTableName: String = getProperty("elencoFlussiDettaglioDeltaNegativo.tableName")
  def getPdrDettaglioGiroContatoreTableName: String = getProperty("pdrDettaglioGiroContatore.tableName")
  def getElencoFlussiDettaglioGiroContatoreTableName: String = getProperty("elencoFlussiDettaglioGiroContatore.tableName")

  // Nome della pubblicazione e della sessione
  def getPublicationType: String = getProperty("publication.type")
  def getZipSessionName: String = getProperty("zip.session.name")

  // Numero di righe massimo per CSV e dimensione massima per ZIP
  def getMaxDimensionZipFileByte: String = getProperty("maxDimensionZipFileByte")
  def getMaxNumRowFile: String = getProperty("maxNumRowFile")
  def getMaxSizeThresholdZip: String = getProperty("maxSizeThresholdZip")

  // Tabelle in lettura
  def getDailyConsumptionTableName: String = getProperty("agg.dailyConsumption.tableName")
  def getDailyConsumptionExecutionid: String = getProperty("agg.dailyConsumption.executionid")
  def getDailyConsumptionIncoerentiTableName: String = getProperty("agg.dailyConsumptionIncoerenti.tableName")
  def getDailyConsumptionEsclusiTableName: String = getProperty("agg.dailyConsumptionEsclusi.tableName")
  def getValidatedFlowTableName: String = getProperty("agg.validateFlow.tableName")
  def getRcugasSospensioniTableName: String = getProperty("rcugas.sospensioni.tableName")
  def getRcugasPdrTableName: String = getProperty("rcugas.pdr.tableName")
  def getRcugasVarMisuratoreTableName: String = getProperty("rcugas.varMisuratore.tableName")
  def getRcugasRemiAnagraficaTableName: String = getProperty("rcugas.remiAnagrafica.tableName")
  def getRcugasGestoreTrasportoTableName: String = getProperty("rcugas.gestTrasporto.tableName")
  def getRcugasVRcugasItTableName: String = getProperty("rcugas.vIt.tableName")
  def getRcugasConnessioniDistrTableName: String = getProperty("rcugas.connDistr2.tableName")

  // Tabelle in scrittura nel modulo query-report-sbg
  def getQueryAggregatoHdfsPath: String = getProperty("hdfs.output.aggregato")
  def getQueryEsclusiHdfsPath: String = getProperty("hdfs.output.esclusi")
  def getQueryElencoFlussiDettaglioEsclusiHdfsPath: String = getProperty("hdfs.output.elencoFlussiDettaglioEsclusi")
  def getQueryIncoerentiHdfsPath: String = getProperty("hdfs.output.incoerenti")
  def getQueryPdrDettaglioIncoerentiHdfsPath: String = getProperty("hdfs.output.pdrDettaglioIncoerenti")
  def getQuerySospesiHdfsPath: String = getProperty("hdfs.output.sospesi")
  def getQueryPdrDettUniHdfsPath: String = getProperty("hdfs.output.pdrDettaglioUnico")
  def getQueryElencoFlussiDettUniHdfsPath: String = getProperty("hdfs.output.elencoFlussoDettaglioUnico")
  def getQueryElencoFlussiDettaglioIncoerentiHdfsPath: String = getProperty("hdfs.output.elencoFlussiDettaglioIncoerenti")
  def getQueryPdrDettaglioDeltaNegativoHdfsPath: String = getProperty("hdfs.output.pdrDettaglioDeltaNegativo")
  def getQueryElencoFlussiDettaglioDeltaNegativoHdfsPath: String = getProperty("hdfs.output.elencoFlussiDettaglioDeltaNegativo")
  def getQueryPdrDettaglioGiroContatoreHdfsPath: String = getProperty("hdfs.output.pdrDettaglioGiroContatore")
  def getQueryElencoFlussiDettaglioGiroContatoreHdfsPath: String = getProperty("hdfs.output.elencoFlussiDettaglioGiroContatore")

  def isPdrListaCEnabled: String = getProperty("pdr.anomali.listaC.enabled")
  def isPdrListaDEnabled: String = getProperty("pdr.anomali.listaD.enabled")
  def getPdrListaCCsvPath: String = getProperty("pdr.anomali.listaC.csvPath")
  def getPdrListaDCsvPath: String = getProperty("pdr.anomali.listaD.csvPath")

  def getClassGroupMeasureRangeMaxPath: String = getProperty("class.group.measure.range.max.path")
  def getNumberOfDaysThresholdForGdm: String = getProperty("incoerenzaGDM.numberOfDays.threshold")
}