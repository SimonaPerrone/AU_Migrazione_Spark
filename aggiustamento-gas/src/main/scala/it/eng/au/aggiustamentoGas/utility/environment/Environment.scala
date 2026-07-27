package it.eng.au.aggiustamentoGas.utility.environment

import it.eng.au.aggiustamentoGas.model.agg._
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.rcugas._
import it.eng.au.aggiustamentoGas.utility.kryo.AggKryoRegistrator
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.convertStringToPeriod
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.joda.time.base.BaseSingleFieldPeriod

import java.io.{File, FileInputStream}
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import java.util.Properties
import scala.util.Try

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")     // utilizza Kryo come serializziatore per le classe registrate qui sotto
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .config("spark.kryo.registrator", classOf[AggKryoRegistrator].getName)
      .config("spark.kryo.classesToRegister", Array(classOf[Flow], classOf[FlowWithInfo], classOf[Consumption],
        classOf[ExternalDailyInfo], classOf[RcuGasMassivoP], classOf[RcuGasConnessioniDistr2], classOf[RcuGasSuspendedPdr],
        classOf[RcuGasTech], classOf[RcuGasVarPrelAnnuoP], classOf[RcuGasVarProfiloP], classOf[Bill2], classOf[Conn2]
        //, classOf[PdrInfo], classOf[PdrInfoSegment]
      )
        .map(_.getName).mkString(","))
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

  def getFs: FileSystem = property.fs

  def applicationName: String = property.applicationName
  def logName: String = property.logName

  //date to run
  def getFlowStartDate: String = getProperty("flow.read.startDate")
  def getFlowEndDate: String = getProperty("flow.read.endDate")
  def getFlowGhigliottina: String = getProperty("flow.read.ghigliottina")
  def getPeriodStartDate: String = getProperty("period.read.startDate")
  def getPeriodEndDate: String = getProperty("period.read.endDate")
  def getRcugasSqoopDate: String = getProperty("rcugas.sqoop.date")

  //session
  def getSession: String = getProperty("session")

  //tratment calcMode
  def getTreatmentCalcMode: String = getProperty("treatment.calcMode")

  //duplicate filter
  def isDuplicateFilterEnabled: String = getProperty("filter.duplicateMeasures.enable")
  def isDuplicateFilterGroupByFilePathEnabled: String = getProperty("filter.duplicateMeasures.groupByFilePath.enable")
  def isDuplicateFilterGroupByTimestampEnabled: String = getProperty("filter.duplicateMeasures.groupByTimestamp.enable")
  def isDuplicateFilterGroupByFileNameEnabled: String = getProperty("filter.duplicateMeasures.groupByFileName.enable")

  //autolettura filter
  def isFilterAutoletturaTreatmentGEnabled: String = Try(getProperty("filter.autolettura.treatmentG.enable")).getOrElse("false")

  //exclusion filter
  def isExclusionFilterEnabled: String = getProperty("filter.exclusion.enabled")
  def getExclusionFilterPath: String = getProperty("filter.exclusion.file.path")
  def isStrongExclusionFilterEnabled: String = getProperty("filter.strongExclusion.enabled")
  def getStrongExclusionFilterFolder: String = getProperty("filter.strongExclusion.folder.path")
  def getStrongExclusionFilterBackupTable: String = getProperty("filter.strongExclusion.backupTable")

  def isForceExclusionFilterEnabled: String = getProperty("filter.forceExclusion.enabled")
  def getForceExclusionFilterPath: String = getProperty("filter.forceExclusion.file.path")
  def isStrongForceExclusionFilterEnabled: String = getProperty("filter.strongForceExclusion.enabled")
  def getStrongForceExclusionFilterFolder: String = getProperty("filter.strongForceExclusion.folder.path")

  //inclusion filter
  def isIdDistrInclusionFilterEnabled: String = getProperty("filter.inclusion.id_distr.enabled")
  def isIdDistrPivaUddInclusionFilterEnabled: String = getProperty("filter.inclusion.id_distr_piva_udd.enabled")
  def isPdrInclusionFilterEnabled: String = getProperty("filter.inclusion.pdr.enabled")
  def getInclusionFilterPath: String = getProperty("filter.inclusion.file.path")

  //internal properties
  def getNumberPartition: String = getProperty("dataset.numberPartition")
  def getBroadcastThreshold: String = getProperty("broadcast.threshold")

  //measures
  def getRglParquetPath: String = getProperty("flow.dataset.rgl.basePath")
  def getRmlParquetPath: String = getProperty("flow.dataset.rml.basePath")
  def getRmvParquetPath: String = getProperty("flow.dataset.rmv.basePath")
  def getRslParquetPath: String = getProperty("flow.dataset.rsl.basePath")
  def getSw1ParquetPath: String = getProperty("flow.dataset.sw1.basePath")
  def getTalParquetPath: String = getProperty("flow.dataset.tal.basePath")
  def getTavParquetPath: String = getProperty("flow.dataset.tav.basePath")
  def getTasParquetPath: String = getProperty("flow.dataset.tas.basePath")
  def getTglParquetPath: String = getProperty("flow.dataset.tgl.basePath")
  def getTmlParquetPath: String = getProperty("flow.dataset.tml.basePath")
  def getTmvParquetPath: String = getProperty("flow.dataset.tmv.basePath")
  def getIm1ParquetPath: String = getProperty("flow.dataset.im1.basePath")
  def getA01ParquetPath: String = getProperty("flow.dataset.a01.basePath")
  def getA40ParquetPath: String = getProperty("flow.dataset.a40.basePath")
  def getSm1ParquetPath: String = getProperty("flow.dataset.sm1.basePath")
  def getSm1rParquetPath: String = getProperty("flow.dataset.sm1r.basePath")
  def getSm2ParquetPath: String = getProperty("flow.dataset.sm2.basePath")
  def getSm2rParquetPath: String = getProperty("flow.dataset.sm2r.basePath")
  def getFuiParquetPath: String = getProperty("flow.dataset.fui.basePath")
  def getA01rParquetPath: String = getProperty("flow.dataset.a01r.basePath")
  def getA02ParquetPath: String = getProperty("flow.dataset.a02.basePath")
  def getA02rParquetPath: String = getProperty("flow.dataset.a02r.basePath")
  def getA40rParquetPath: String = getProperty("flow.dataset.a40r.basePath")
  def getD01rParquetPath: String = getProperty("flow.dataset.d01r.basePath")
  def getD01ParquetPath: String = getProperty("flow.dataset.d01.basePath")
  def getD02rParquetPath: String = getProperty("flow.dataset.d02r.basePath")
  def getD02ParquetPath: String = getProperty("flow.dataset.d02.basePath")
  def getAd2rParquetPath: String = getProperty("flow.dataset.ad2r.basePath")
  def getAd2ParquetPath: String = getProperty("flow.dataset.ad2.basePath")
  def getAd3rParquetPath: String = getProperty("flow.dataset.ad3r.basePath")
  def getAd3ParquetPath: String = getProperty("flow.dataset.ad3.basePath")
  def getAd4rParquetPath: String = getProperty("flow.dataset.ad4r.basePath")
  def getAd4ParquetPath: String = getProperty("flow.dataset.ad4.basePath")
  def getAd5rParquetPath: String = getProperty("flow.dataset.ad5r.basePath")
  def getAd5ParquetPath: String = getProperty("flow.dataset.ad5.basePath")
  def getS02rParquetPath: String = getProperty("flow.dataset.s02r.basePath")
  def getS02ParquetPath: String = getProperty("flow.dataset.s02.basePath")
  def getS40rParquetPath: String = getProperty("flow.dataset.s40r.basePath")
  def getS40ParquetPath: String = getProperty("flow.dataset.s40.basePath")
  def getR01rParquetPath: String = getProperty("flow.dataset.r01r.basePath")
  def getR01ParquetPath: String = getProperty("flow.dataset.r01.basePath")
  def getR40rParquetPath: String = getProperty("flow.dataset.r40r.basePath")
  def getR40ParquetPath: String = getProperty("flow.dataset.r40.basePath")
  def getM01rParquetPath: String = getProperty("flow.dataset.m01r.basePath")
  def getM01ParquetPath: String = getProperty("flow.dataset.m01.basePath")
  def getV01rParquetPath: String = getProperty("flow.dataset.v01r.basePath")
  def getV01ParquetPath: String = getProperty("flow.dataset.v01.basePath")
  def getV02rParquetPath: String = getProperty("flow.dataset.v02r.basePath")
  def getV02ParquetPath: String = getProperty("flow.dataset.v02.basePath")
  def getIgmgParquetPath: String = getProperty("flow.dataset.igmg.basePath")
  def getIgmrParquetPath: String = getProperty("flow.dataset.igmr.basePath")
  def getDefParquetPath: String = getProperty("flow.dataset.def.basePath")

  //rcu tables
  def getRcuAziendaPath: String = getProperty("rcuAziendaP.basepath")

  //rcugas tables
  def getRcugasBilanciamentoPath: String = getProperty("rcugasBilanciamentoP.basepath")
  def getRcugasConnessioniDistr2RemiPath: String = getProperty("rcugasConnessioniDistr2Remi.basepath")
  def getRcugasDistributorePath: String = getProperty("rcugasDistributoreP.basepath")
  def getRcugasFornituraPath: String = getProperty("rcugasFornituraP.basepath")
  def getRcugasGasltPath: String = getProperty("rcugasItP.basepath")
  def getRcugasGestTrasportPath: String = getProperty("rcugasGestTrasportoP.basepath")
  def getRcugasMassivoPath: String = getProperty("rcugasMassivoP.basepath")
  def getRcugasPdrDatiPrelievoPath: String = getProperty("rcugasPdrDatiPrelievo.basepath")
  def getRcugasRemiAggregazionePath: String = getProperty("rcugasRemiAggregazioneP.basepath")
  def getRcugasRemiAnagraficaPath: String = getProperty("rcugasRemiAnagraficaP.basepath")
  def getRcugasSospensioniPath: String = getProperty("rcugasSospensioniP.basepath")
  def getRcugasUdbPath: String = getProperty("rcugasUdbP.basepath")
  def getRcugasVarMisuratorePath: String = getProperty("rcuGasVarMisuratoreP.basepath")
  def getRcugasVarConvertitorePath: String = getProperty("rcuGasVarConvertitoreP.basepath")
  def getRcugasVarPrelAnnuoPath: String = getProperty("rcuGasVarPrelAnnuoP.basepath")
  def getRcugasVarProfiloPath: String = getProperty("rcuGasVarProfiloP.basepath")
  def getRcugasVarTrattamentoPath: String = getProperty("rcuGasVarTrattamentoP.basepath")

  // settlegas tables
  def getTabProfiliStdPercPath: String = getProperty("tab_profili_giorn_std_perc.basepath")
  def getTabProfiliStdPercTableName: String = getProperty("tab_profili_giorn_std_perc.tableName")

  //agg tables
  def getMonthTreatmentTable: String = getProperty("monthTreatment.table")
  def getMonthTreatmentPath: String = getProperty("monthTreatment.basepath")
  def getValidatedFlowsTable: String = getProperty("validatedFlows.table")
  def getValidatedFlowsPath: String = getProperty("validatedFlows.basepath")
  def getDailyConsumptionTable: String = getProperty("dailyConsumption.table")
  def getDailyConsumptionExclTable: String = getProperty("dailyConsumptionExcl.table")
  def getDailyConsumptionIncGdMTable: String = getProperty("dailyConsumptionIncGdM.table")
  def getDailyConsumptionPath: String = getProperty("dailyConsumption.basepath")
  def getDailyConsumptionExclPath: String = getProperty("dailyConsumptionExcl.basepath")
  def getDailyConsumptionIncGdMPath: String = getProperty("dailyConsumptionIncGdM.basepath")
  def getSegmentTable: String = getProperty("segment.table")
  def getSegmentPath: String = getProperty("segment.basepath")

  //tdg coeff k table
  def getTdgCoeffKPath: String = getProperty("tdgCoeffK.basepath")
  def getTdgCoeffKTable: String = getProperty("tdgCoeffK.table")

  //moltiplicatori consumi incoerenti
  def getSterilizeMeasureEnable: String = getProperty("sterilize.measure.enable")
  def getClassGroupMeasureRangeMaxPath: String = getProperty("class.group.measure.range.max.path")
  def getxMultiplierMaxRange: String = getProperty("multiplier.max.range.x")
  def getyMultiplierConsumptionSterilize: String = getProperty("multiplier.consumption.sterilize.y")


  def getPeriodStartDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("period.read.startDate.time.back"))
  def getPeriodEndDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("period.read.endDate.time.back"))
  def getFlowStartDateTimeBackWrtPeriod: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("flow.read.startDate.time.back.wrt.period.read.startDate"))
  def getFlowStartDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("flow.read.startDate.time.back"))
  def getFlowEndDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("flow.read.endDate.time.back"))
  def getFlowGhigliottinaTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("flow.read.ghigliottina.time.back"))
  def getRcugasSqoopDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("rcugas.sqoop.date.time.back"))

  def getNumberOfDaysThresholdForGdm: String = getProperty("incoerenzaGDM.numberOfDays.threshold")

  def getCarriBombolaiFilePath: String = getProperty("carriBombolai.basepath")
}