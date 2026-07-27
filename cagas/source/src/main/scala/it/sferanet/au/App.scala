package it.sferanet.au

import it.sferanet.au.controller.PdrPartitioner
import it.sferanet.au.controller.ca.{CaController, ConsumptionController}
import it.sferanet.au.controller.caFinal.{CaFinalController, CaPreFinalController, PdrMassivoController}
import it.sferanet.au.controller.exclusionpdr.ExclusionPdrController
import it.sferanet.au.controller.forcing.{ForcedController, ForcedDedottiController}
import it.sferanet.au.controller.tds.TdsController
import it.sferanet.au.controller.validation.ValidationController
import it.sferanet.au.dal._
import it.sferanet.au.dal.output.{CATable, ConsumptionTable, ValidationTable}
import it.sferanet.au.dal.rcu.{RcuGasConnessioniTable, RcuGasProfiloTable, RcuGasTable, RcuGasTechTable}
import it.sferanet.au.dal.rcugas.{RcuAziendaDao, RcugasDistributoreDao, RcugasUdbDao}
import it.sferanet.au.filterPdr.ingestionFilter.IngestionFilter
import it.sferanet.au.filterPdr.{FilterPdrFactory, MeasureFilter}
import it.sferanet.au.model.Flow
import it.sferanet.au.utilities.HDFSUtils.deleteIfExist
import it.sferanet.au.utilities.args.FlowArgsFactory
import it.sferanet.au.utilities.environment.{CaEnvironment, CcgFinEnvironment, CcgRicEnvironment}
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object App {
  private val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val flowArgsConfig = FlowArgsFactory.parse(args)

    if (flowArgsConfig.session.contains("CCG_FIN"))
      CcgFinEnvironment.setEnvironment(flowArgsConfig)
    else if (flowArgsConfig.session.contains("CCG_RIC"))
      CcgRicEnvironment.setEnvironment(flowArgsConfig)
    else
      CaEnvironment.setEnvironment(flowArgsConfig)

    run()

    log.warn("CA procedure terminated.")
  }

  def run(): Unit = {
    // Lettura delle properties riguardanti la tds
    val tdsEndDateString = Environment.getTdsReceiveEndDate
    val tdsEndDate = Constants.getDate(Constants.getFormatter("yyyy-MM-dd"), tdsEndDateString).get
    val tdsLastUpdatedDate = Constants.getDate(Constants.getFormatter("yyyy-MM-dd"), Environment.getTdsLastUpdatedDate).get

    val defaultPartitioner: Partitioner = new PdrPartitioner(Environment.getNumberPartition.toInt)

    // Lettura della massivo, contenente le informazioni di tutti i PdR attivi (pdr, inizio, fine, matricola, etc)
    val pdrMassivoController = new PdrMassivoController()
    val pdrMassivoDF = pdrMassivoController.get()

    // Estrazione delle misure dalle tabelle di misura
    // Flow: rappresenta i dati di una misura
    val measures: RDD[Flow] = extractMeasures()

    // Lettura della modalità di lancio (massiva o con filtri) e applicazione dei filtri se presenti
    val (flowTmpRDD1, pdrMassivoDFFiltered) = FilterPdrFactory.getLaunchMode(Environment.getFilterPdrMode, measures, pdrMassivoDF)
    // Applicazione delle forzature, se attive
    val pdrMassivoDFFilteredForced = if (Environment.isForcingEnabled.equals("true")) ForcedController.forcing(pdrMassivoDFFiltered) else pdrMassivoDFFiltered
    // Applicazione delle forzature, se attive, ai PdR dedotti
    val pdrMassivoDFFilteredForcedDedotti = if (Environment.isForcingDedottiEnabled.equals("true")) ForcedDedottiController.forcing(pdrMassivoDFFilteredForced) else pdrMassivoDFFilteredForced

    // Applicazione di eventuali filtri di esclusione
    val (measurePdrExclusion, pdrMassivoExclusion) =
      if (Environment.isExclusionFilterEnabled.equals("true"))
        ExclusionPdrController.exclude(flowTmpRDD1, pdrMassivoDFFilteredForcedDedotti)
      else
        (flowTmpRDD1, pdrMassivoDFFilteredForcedDedotti)

    // Exclude some measures iff ignorePdrMeasure.enable=true
    val flowTmpRDD2 = MeasureFilter.excludeMeasures(measurePdrExclusion)

    // Esecuzione funzione di "lead" per ottenere soltanto l'ultima versione delle misure caricate in Hive
    val flowRDD = ValidationController.getLastLoadingMeasuresVersion(flowTmpRDD2)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Lettura delle tabelle da rcu e rcugas utili al calcolo dei consumi
    val rcuGasTable = new RcuGasTable(Environment.getRcugasMassivoPath, Environment.getMassivoExecutionId, RcuGasTable.flowFields)
    val rcuGasRDD = rcuGasTable.get().keyBy(_.t_codice_pdr).partitionBy(defaultPartitioner).map(_._2)
    val rcuGasTechTable = new RcuGasTechTable(Environment.getRcugasTechPath, Environment.getMassivoExecutionId, RcuGasTechTable.flowFields)
    val rcuGasTechRDD = rcuGasTechTable.get().keyBy(_.t_codice_pdr).partitionBy(defaultPartitioner).map(_._2)
    val rcuGasProfiloTable = new RcuGasProfiloTable(Environment.getRcugasVarProfiloPath)
    val rcuGasProfiloRDD = rcuGasProfiloTable.get()
    val rcuGasConnessioniDistrTable = new RcuGasConnessioniTable(Environment.getRcugasConnessioniDistr2Path)
    val rcuGasConnessioniDF = rcuGasConnessioniDistrTable.get()
    val distributoreDF = new RcugasDistributoreDao().readTable
    val aziendaDF = new RcuAziendaDao().readTable
    val udbDF = new RcugasUdbDao().readTable
    val readVsg = Environment.getSqlContext.read.parquet(Environment.getPrtVsgPath)
    val readVtg = Environment.getSqlContext.read.parquet(Environment.getPrtVtgPath)
    val readVsgAggRcu = Environment.getSqlContext.read.parquet(Environment.getPrtVsgAggRcuPath)
    val readVtgAggRcu = Environment.getSqlContext.read.parquet(Environment.getPrtVtgAggRcuPath)

    val weightsTable = new WeightsTable(Environment.getWeightsPath, WeightsTable.flowFields)
    val weights = weightsTable.get()
    val weightsRDD = weights._1
    val weightsDFPreRemi = weights._2
    val weightsDFPostRemi = weights._3

    if (Environment.isLocalMode) {
      deleteIfExist(Environment.getValidationPath)
      deleteIfExist(Environment.getConsumptionPath)
      deleteIfExist(Environment.getCaDatasetPath)
    }

    log.warn("Calcolo zona climatica.")
    val lookupZonaClimatica = Environment.getSparkContext.broadcast(Environment.getSqlContext.read
      .parquet(Environment.getLookupZonaClimaticaPath)
      .rdd
      .map(r =>
        (r.getAs("t_codice_istat").toString, r.getAs("t_regione_climatica").toString)
      ).collectAsMap())

    // Lettura della tabella gas_tds in cui sono presenti le informazioni della tds (classe_prelievo e cat_uso)
    val tdsTable = TdsController.getTdsTable(Environment.getSettleGasTdsPath, tdsEndDate, isForDedotti = false, tdsLastUpdatedDate)

    // Se siamo nel caso di lancio massivo (PRE o AGG_FIN) o di filtro Agg Ric, allora occorre filtrare la gas_tds come segue
    val pdrsToRemoveDF = TdsController.getPdrsToRemove(readVsg, readVtg, readVsgAggRcu, readVtgAggRcu, tdsEndDateString)

    val pdrsToRemove = if (List("PRE", "AGG_FIN", "AGG_RIC").contains(Environment.getTipoTrasmissione)) {
      TdsController.getPdrsToRemoveRDD(pdrsToRemoveDF)
    } else None

    val tdsPrepare = if (List("PRE", "AGG_FIN", "AGG_RIC").contains(Environment.getTipoTrasmissione)) {
      // We persist pdrsToRemove because it's used at the end of procedure as well
      pdrsToRemove.get.persist(StorageLevel.MEMORY_AND_DISK)
      TdsController.filterTds(tdsTable, pdrsToRemove.get)
    }
    else tdsTable
      .persist(StorageLevel.MEMORY_AND_DISK)

    log.warn("Calcolo TDS")
    val tds = Environment.getSparkContext.broadcast(tdsPrepare.collectAsMap())


    /** VALIDAZIONE */
    // Selezione delle misure validate, ovvero le misure che concorrono al calcolo della ca
    val validationController = new ValidationController
    val validatedMeausureRDD = validationController.getMeasures(flowRDD).persist(StorageLevel.MEMORY_AND_DISK)

    // Creazione e scrittura della tabella validated_flows
    log.warn("Scrittura tabella Validation")
    val validationTable = new ValidationTable(Environment.getSession, Environment.executionId)
    val validationDF = validationTable.createDataFrame(validatedMeausureRDD, tds)
    validationTable.write(validationDF)

    if (Constants.DEBUG)
      log.info("validatedMeausureRDD.count(): " + validatedMeausureRDD.count())

    flowRDD.unpersist(blocking = false)

    /** Calcolo CONSUMI */
    // Calcolo dei consumi
    val consumptionController = new ConsumptionController()
    val consumptions = consumptionController.execute(validatedMeausureRDD, rcuGasTechRDD, rcuGasRDD, rcuGasProfiloRDD)
      .persist(StorageLevel.MEMORY_AND_DISK)
    if (Constants.DEBUG)
      log.info("consumptions.count(): " + consumptions.count())

    // Creazione e scrittura della tabella dei consumi
    log.warn("Scrittura tabella Consumption")
    val consumptionTable = new ConsumptionTable(Environment.getSession, Environment.executionId)
    val consumptionDF = consumptionTable.createDataFrame(consumptions.flatMap(v => v._2._1))
    consumptionTable.write(consumptionDF) //scrivo i consumi

    validatedMeausureRDD.unpersist(blocking = false)

    /** Calcolo CA */
    // Calcolo della ca, ovvero del consumo annuo
    val caController = new CaController()
    val ca = caController.execute(consumptions, lookupZonaClimatica)

    // Creazione e scrittura della tabella ca
    log.warn("Scrittura tabella CA")
    val caTable = new CATable(Environment.getSession, Environment.executionId)
    caTable.deleteTemporaryTableContents()
    val caDF = caTable.parseCaToDF(ca)
    val caDFWithRemi = caController.getValidRemi(caDF, rcuGasConnessioniDF)

    if (Constants.DEBUG)
      log.info("ca.count(): " + ca.count())

    caTable.writeTempTable(caDFWithRemi)

    ca.unpersist(blocking = true)

    val caDFtemp = caTable.readTmpTable()
    val caDFEnriched = caController.joinWeights(caDFtemp, weightsDFPreRemi, weightsDFPostRemi)
    caTable.writeTempTable2(caDFEnriched)

    consumptions.unpersist(blocking = true)

    val caDFReload = caTable.readTmpTable2()
    val caDFCodProfRecalc = caController.applyForcedRecalculation(caDFReload)
    val caDFRecalc = caController.executeRecalculation(caDFCodProfRecalc, weightsDFPreRemi, weightsDFPostRemi)

    // Lettura della tabella gas_tds nel caso dei dedotti
    val tdsTableForDedotti = TdsController.getTdsTable(Environment.getSettleGasTdsPath, tdsEndDate, isForDedotti = true, tdsLastUpdatedDate)

    // Filtraggio della tabella gas_tds rimuovendo i PdR estratti dalle tabelle prt_vtg e prt_vsg
    val tdsPrepareForDedotti = if (List("PRE", "AGG_FIN", "AGG_RIC").contains(Environment.getTipoTrasmissione)) {
      TdsController.filterTds(tdsTableForDedotti, pdrsToRemove.get)
    } else tdsTableForDedotti

    /** Utilizzata per associare ai PdR dedotti le informazioni più recenti dalla gas_tds, se presenti successivamente al calcolo dell'ultima cod_prof_std_da_tds (AU-507) */
    val tdsForDedotti = TdsController.convertToDF(tdsPrepareForDedotti)
    /** Utilizzata per associare ai PdR dedotti la presenza o meno di una tds (AU-494) */
    val tdsForPresTds = TdsController.convertToDFForPresTds(tdsPrepare)

    val caDFwithTds = caController.joinSchedaTds(caDFRecalc, tdsForDedotti)

    caTable.write(caDFwithTds)

    /** Calcolo ca_pre_final */
    // Creazione e scrittura della tabella ca_pre_final
    log.warn("Scrittura tabella PreFinal")
    val caPreFinalController = new CaPreFinalController(Environment.getSession, Environment.executionId)
    val caPreFinalDF = caPreFinalController.get(pdrMassivoExclusion, caDFwithTds, tdsForPresTds, tdsForDedotti).persist(StorageLevel.MEMORY_AND_DISK) // .cache()
    caPreFinalController.write(caPreFinalDF)

    caPreFinalDF.unpersist(blocking = false)

    /** Calcolo ca_final */
    // Creazione e scrittura della ca_final
    log.warn("Scrittura tabella Final")
    val caFinalController = new CaFinalController
    val caFinalDF = caFinalController.get(caPreFinalDF, distributoreDF, aziendaDF, udbDF).persist(StorageLevel.MEMORY_AND_DISK) // .cache()
    caFinalController.write(caFinalDF)
    //caFinalController.writeToExport(caFinalDF)

    caTable.deleteTemporaryTableContents()

  }

  /**
   * Extracts and filters measures to obtain an RDD of Flow instances.
   *
   * The method performs the following operations:
   *   - For each flow in MeasureType get the corresponding measures and combines them using union.
   *   - Filters out flows with an invalid dateLoadFromLocalFile.
   *   - Filters out flows with a dateLoadFromLocalFile greater than the receive end date.
   *   - Filters out flows with null or empty pdr values.
   *
   * @return an RDD of filtered Flow instances
   */
  def extractMeasures(): RDD[Flow] = {
    val receiveEndDate = Environment.getFlowReceiveEndDate.toInt

    val flows = MeasureType.values.map(FlowFactory.build)
      .reduce(_.union(_))
      .coalesce(Environment.getNumberPartition.toInt)
      .repartition(Environment.getNumberPartition.toInt)
      .filter(f => f.dateLoadFromLocalFile != -1)
      .filter(f => f.dateLoadFromLocalFile <= receiveEndDate)
      .filter(f => f.date.isDefined && (f.pdr != null && !f.pdr.trim.equals("null") && !f.pdr.trim.equals("")))

    IngestionFilter.removeDuplicateFlows(flows)
  }
}
