package it.sferanet.au.utilities


import it.sferanet.au.model.Flow
import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.DateUtils.convertStringToPeriod
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.joda.time.base.BaseSingleFieldPeriod

import java.io.{File, FileInputStream}
import java.util.{Date, Properties}

class Environment(appName: String, propertiesPath: String, isLocalMode: Boolean) {

  private val isLocal: Boolean = isLocalMode

  if (!isLocal && Constants.DEBUG)
    throw new RuntimeException("Debug mode in production")

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
//      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .config("spark.kryo.classesToRegister", Array(
        classOf[Flow],
        classOf[Tal],
        classOf[Tas],
        classOf[Tav],
        classOf[Tgl],
        classOf[Tml],
        classOf[A01],
        classOf[A02],
        classOf[A40],
        classOf[AD2],
        classOf[AD3],
        classOf[AD4],
        classOf[AD5],
        classOf[FDD],
        classOf[FUI],
        classOf[IgmgPost],
        classOf[IgmgPre],
        classOf[Im1Post],
        classOf[Im1Pre],
        classOf[M01],
        classOf[R01],
        classOf[R40],
        classOf[Rmv],
        classOf[Rsl],
        classOf[S02],
        classOf[S40],
        classOf[Sm1],
        classOf[Sw1],
        classOf[Swg1],
        classOf[Tmv],
        classOf[V01],
        classOf[V02],
        classOf[A01R],
        classOf[A02R],
        classOf[A40R],
        classOf[AD2R],
        classOf[AD3R],
        classOf[AD4R],
        classOf[AD5R],
        classOf[M01r],
        classOf[R01r],
        classOf[R40r],
        classOf[Rgl],
        classOf[Rml],
        classOf[S02R],
        classOf[S40R],
        classOf[SM1R],
        classOf[V01R],
        classOf[V02R])
        .map(_.getName).mkString(","))
      .enableHiveSupport()
      .getOrCreate()
  }

  private val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

  private val prop = new Properties()
  if (isLocal)
    prop.load(new FileInputStream(new File(propertiesPath)))
  else
    prop.load(fs.open(new Path(propertiesPath)).getWrappedStream)

  //sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", prop.getProperty("dataset.numberPartition")) //done after settings are loaded
  //sparkSession.sparkContext.getConf.set("spark.sql.shuffle.partitions", prop.getProperty("dataset.numberPartition"))
  //sparkSession.conf.set("spark.sql.shuffle.partitions", prop.getProperty("dataset.numberPartition"))
}

object Environment {
  val executionId: Long = System.currentTimeMillis()

  private var caEnvironment: Environment = _

  def isLocalMode: Boolean = caEnvironment.isLocal

  def getOrCreate(appName: String, path: String, isLocal: Boolean = false): Unit = caEnvironment = new Environment(appName, path, isLocal)
  def getProperty(key: String): String = caEnvironment.prop.getProperty(key)
  def setProperty(key: String, value: String): Unit = caEnvironment.prop.setProperty(key, value)
  def getSparkProperty(key: String): Option[String] = caEnvironment.sparkSession.sparkContext.getConf.getOption(key)

  def printProperties: String = caEnvironment.prop.toString.replace("{","").replace("}","").replace(" ","").split(",").mkString("\n")

  def getSpark: SparkSession = caEnvironment.sparkSession
  def getFs: FileSystem = caEnvironment.fs
  def getSqlContext: SQLContext = getSpark.sqlContext
  def getSparkContext: SparkContext = getSpark.sparkContext

  val getPartitionDate: Date = new Date(executionId)

  //date to run
  def getFlowStartDate: String = getProperty("flow.read.startDate")
  def getFlowEndDate: String = getProperty("flow.read.endDate")
  def getTdsReceiveEndDate: String = getProperty("tds.read.receive.endDate")
  def getTdsLastUpdatedDate: String = getProperty("tds.read.lastUpdatedDate")
  def getFlowReceiveEndDate: String = getProperty("flow.read.receive.endDate")

  //other dates
  def getContractContuinityUpperBoundDate: String = getProperty("contractContinuity.upperBound.data")
  def getZInfDate: String = getProperty("z.inf.date")
  def getZSupDate: String = getProperty("z.sup.date")


  //dates for CCG
  def getFlowStartDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("flow.read.startDate.time.back"))
  def getFlowEndDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("flow.read.endDate.time.back"))
  def getFlowReadReceiveEndDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("flow.read.receive.endDate.time.back"))
  def getTdsReadReceiveEndDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("tds.read.receive.endDate.time.back"))
  def getContractContinuityUpperBoundDateTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("contractContinuity.upperBound.data.time.back"))
  def getZInfDateTimeBackWrtZSupDate: BaseSingleFieldPeriod = convertStringToPeriod(getProperty("z.inf.date.time.back.wrt.z.sup.date"))

  //internal properties
  def getNumberPartition: String = getProperty("dataset.numberPartition")
  def getTipoTrasmissione: String = getProperty("tipoTrasmissione.value")
  def getSession: String = getProperty("sessione")

  //filter mode
  def getFilterPdrMode: String = getProperty("filterPdr.mode")
  def getFilterPdrCsvPath: String = getProperty("filterPdr.pdr.path")
  def getFilterPdrDistribuzioneCsvPath: String = getProperty("filterPdr.distributore.path")
  def getFilterPdrUddCsvPath: String = getProperty("filterPdr.udd.path")
  def getFilterPdrUdbCsvPath: String = getProperty("filterPdr.udb.path")

  //inclusion and exclusion filters
  def isExclusionFilterEnabled: String = getProperty("exclusion.pdr.filter")
  def getExclusionFilterCsvPath: String = getProperty("exclusion.pdr.filter.csv.path")

  //duplicates measures filter
  def isDuplicateMeasuresFilterEnabled: String = getProperty("filter.duplicateMeasures.enable")
  def isDuplicateFilterGroupByFilePathEnabled: String = getProperty("filter.duplicateMeasures.groupByFilePath.enable")
  def isDuplicateFilterGroupByTimestampEnabled: String = getProperty("filter.duplicateMeasures.groupByTimestamp.enable")
  def isDuplicateFilterGroupByFileNameEnabled: String = getProperty("filter.duplicateMeasures.groupByFileName.enable")

  //forced mode
  def isForcingEnabled: String = getProperty("forced.enable")
  def getForcingCsvPath: String = getProperty("filterPdr.forzatura.path")
  def isForcingDedottiEnabled: String = getProperty("forcedDedotti.enable")
  def getForcingDedottiCsvPath: String = getProperty("filterPdr.forzaturaDedotti.path")

  //Ignore pdr measures
  def isIgnorePdrMeasuresFilterEnabled: String = getProperty("ignorePdrMeasure.enable")
  def getIgnorePdrMeasurePdrCsvPath: String = getProperty("ignorePdrMeasure.pdr.path")
  def getIgnorePdrMeasureFileCsvPath: String = getProperty("ignorePdrMeasure.measureFile.path")
  def getIgnorePdrMeasureCsvPath: String = getProperty("ignorePdrMeasure.pdrMeasureFile.path")
  def getIgnorePdrMeasuresBroadcastThreshold: String = getProperty("ignorePdrMeasure.broadcast.threshold")

  //consumption properties
  def getCeMeanRange: String = getProperty("ce.mean.range")

  //weights
  def getWeightsPath: String = getProperty("weights.basepath")

  //rcugas tables
  def getRcugasMassivoPath: String = getProperty("rcugas.basepath")
  def getRcugasMassivoTableName: String = getProperty("rcugas.massivo.tableName")
  def getRcugasTechPath: String = getProperty("rcugas_tech.basepath")
  def getRcugasVarConvertitorePath: String = getProperty("rcugas_var_convertitore_p.basepath")
  def getRcugasConnessioniDistr2Path: String = getProperty("rcugas_connessioni_distr2__p.basepath")
  def getRcugasBilanciamentoPath: String = getProperty("rcugas_bilanciamento_p.basepath")
  def getRcugasVarTrattamentoPath: String = getProperty("rcugas.var_trattamento.basepath")
  def getRcugasVarPrelAnnuoPath: String = getProperty("rcugas.rcugas_var_prel_annuo_p.basepath")
  def getRcugasVarProfiloPath: String = getProperty("rcugas.rcugas_var_profilo_p.basepath")
  def getRcuGasUdbPath: String = getProperty("rcugas_udb_p.basepath")
  def getRcugasDistributorePath: String = getProperty("v_rcugas_distributore_p.basepath")
  def getRcugasMassivoPPath: String = getProperty("rcugas_massivo__p.basepath")

  //massivo
  def getMassivoAnnoCompetenza: String = getProperty("pdr_massivo.anno_competenza")
  def getMassivoExecutionId: String = getProperty("rcugas.massivo.execution_id")

  //rcu tables
  def getRcuAziendaPath: String = getProperty("rcu_azienda_p.basepath")

  //prt tables
  def getIstatRegioneClimaticaPath: String = getProperty("istat_regione_climatica_p.basepath")
  def getLookupZonaClimaticaPath: String = getProperty("lookup_zonaclimatica.basepath")

  //prt_vsg and prt_vtg tables
  def getPrtVsgPath: String = getProperty("prt_vsg_p.basepath")
  def getPrtVtgPath: String = getProperty("prt_vtg_p.basepath")
  def getPrtVsgAggRcuPath: String = getProperty("prt_vsg_agg_rcu_p.basepath")
  def getPrtVtgAggRcuPath: String = getProperty("prt_vtg_agg_rcu_p.basepath")

  //cdp read tables
  def getCaPath: String = getProperty("ca.basePath")
  def getCaTable: String = getProperty("ca.table_name")
  def getCaFinalTableName: String = getProperty("ca_final.table_name")
  def getCaPreFinalTableName: String = getProperty("ca_pre_final.table_name")
  def getCaPreFinalExecutionId: String = getProperty("ca_pre_final.execution_id")
  def getCodProfStdDaTdsPath: String = getProperty("cod_prof_std_da_tds.basepath")

  //cdp output tables
  def getCaDatasetPath: String = getProperty("ca.dataset.basePath")
  def getCaPreFinalPath: String = getProperty("ca_pre_final.basepath")
  def getCaFinalPath: String = getProperty("ca_final.basepath")
  def getCaFinalToExportPath: String = getProperty("ca_final_to_export.basepath")
  def getValidationPath: String = getProperty("validation.dataset.basePath")
  def getValidationTable: String = getProperty("validation.table_name")
  def getConsumptionPath: String = getProperty("consumption.dataset.basePath")
  def getConsumptionTable: String = getProperty("consumption.table_name")

  //settle_gas tables
  def getGasTdsPath: String = getProperty("gas_tds.basepath")
  def getSettleGasTdsPath: String = getProperty("tds.basepath")

  //tables to extract piva from
  def getRcuAziendaTableName: String = getProperty("rcu.azienda.tableName")
  def getRcugasDistributoreTableName: String = getProperty("rcugas.dist.tableName")
  def getRcugasUdbTableName: String = getProperty("rcugas.udb.tableName")

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
  def getFuiParquetPath: String = getProperty("flow.dataset.fui.basePath")
  def getA01rParquetPath: String = getProperty("flow.dataset.a01r.basePath")
  def getA02ParquetPath: String = getProperty("flow.dataset.a02.basePath")
  def getA02rParquetPath: String = getProperty("flow.dataset.a02r.basePath")
  def getA40rParquetPath: String = getProperty("flow.dataset.a40r.basePath")
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
  def getIm1PostParquetPath: String = getProperty("flow.dataset.im1post.basePath")
  def getIm1PreParquetPath: String = getProperty("flow.dataset.im1pre.basePath")
  def getIgmgPreParquetPath: String = getProperty("flow.dataset.igmgpre.basePath")
  def getIgmgPostParquetPath: String = getProperty("flow.dataset.igmgpost.basePath")
  def getIgmrPreParquetPath: String = getProperty("flow.dataset.igmrpre.basePath")
  def getIgmrPostParquetPath: String = getProperty("flow.dataset.igmrpost.basePath")
  def getFddParquetPath: String = getProperty("flow.dataset.fdd.basePath")
  def getSwg1ParquetPath: String = getProperty("flow.dataset.swg1.basePath")}