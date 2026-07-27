package it.eng.au.aggiustamentoGas

import it.eng.au.aggiustamentoGas.controller.Esclusi.EsclusiController
import it.eng.au.aggiustamentoGas.controller._
import it.eng.au.aggiustamentoGas.controller.classeGdM.{ClassiGruppiDiMisuraPortataRcugas, IncoerentiGdMController}
import it.eng.au.aggiustamentoGas.dao.CarriBombolaiFileDao
import it.eng.au.aggiustamentoGas.dao.agg.{DailyConsumptionDAO, DailyConsumptionEsclusiDAO, DailyConsumptionIncGdMDAO, MonthTreatmentDAO, SegmentDAO, ValidatedFlowDAO}
import it.eng.au.aggiustamentoGas.dao.rcugas._
import it.eng.au.aggiustamentoGas.dao.settlegas.TabProfiliGiorniStdPercDao
import it.eng.au.aggiustamentoGas.dao.tdg.TdgCoeffKDao
import it.eng.au.aggiustamentoGas.filter.exclusion.{ExclusionFilterController, ForceExclusionController}
import it.eng.au.aggiustamentoGas.filter.inclusion.InclusionFilterController
import it.eng.au.aggiustamentoGas.schema.agg.{DailyConsumptionAGGSBGSchema, DailyConsumptionSchema}
import it.eng.au.aggiustamentoGas.utility.args.FlowArgsFactory
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.CCG
import it.eng.au.aggiustamentoGas.utility.constants.TreatmentCalcMode
import it.eng.au.aggiustamentoGas.utility.environment.{AggSetEnvironment, CcgSetEnvironment, Environment}
import it.eng.au.aggiustamentoGas.utility.log.LogUtility
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.storage.StorageLevel

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val flowArgsConfig = FlowArgsFactory.parse(args)
      if (flowArgsConfig.session.contains(CCG)) {
        CcgSetEnvironment.setEnvironment(flowArgsConfig)
      } else
        AggSetEnvironment.setEnvironment(flowArgsConfig)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()
    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }

  /**
   * Esegue tutta la pipeline per il processo di aggiustamento gas
   */
  def run(): Unit = {
    /** Estremo sinistro del periodo di lettura delle misure */
    val startFlowDate = Environment.getFlowStartDate
    /** Estremo destro del periodo di lettura delle misure */
    val endFlowDate = Environment.getFlowEndDate

    /** Estremo sinistro del periodo di calcolo dei consumi */
    val startPeriodDate = Environment.getPeriodStartDate
    /** Estremo destro del periodo di calcolo dei consumi */
    val endPeriodDate = Environment.getPeriodEndDate

    /** Lettura del metodo di assegnazione del trattamento (infered, rcugas) */
    val treatmentCalcMode = TreatmentCalcMode.values.find(_.toString == Environment.getTreatmentCalcMode).getOrElse(TreatmentCalcMode.rcugas)

    val session = Environment.getSession

    // Inizializzazione dei DAO per le tabelle in rcugas (contenente i dati di anagrafica)
    val rcuGasMassivoPDAO = new RcuGasMassivoPDAO
    val rcuGasConnessioniDistr2DAO = new RcuGasConnessioniDistr2DAO
    val rcuSuspendedPdrDAO = new SuspendedPdrDAO
    val rcuGasVarMisuratorePDAO = new RcuGasVarMisuratorePDAO
    val rcuGasVarConvertitorePDAO = new RcuGasVarConvertitorePDAO
    val rcuGasVarPrelAnnuoPDAO = new RcuGasVarPrelAnnuoPDAO
    val rcuGasVarProfiloPDAO = new RcuGasVarProfiloPDAO
    val rcuGasVarTrattamentoPDAO = new RcuGasVarTrattamentoPDAO

    // Inizializzazione dei controller
    val inclusionFilters = InclusionFilterController.getFilter(rcuGasMassivoPDAO.readParquet, rcuGasConnessioniDistr2DAO.readParquet)
    val softExclusionFilterController = new ExclusionFilterController(isStrong = false)
    val strongExclusionFilterController = new ExclusionFilterController(isStrong = true)
    val softForceExclusionFilterController = new ForceExclusionController(isStrong = false)
    val strongForceExclusionFilterController = new ForceExclusionController(isStrong = false)
    val flowController = new FlowController(softExclusionFilterController, strongExclusionFilterController, inclusionFilters)
    val cancelController = new CancelController
    val treatmentController = new TreatmentController
    val priorityController = new PriorityController
    val im1IgmgCorrectionController = new Im1IgmgCorrectionController
    val joinInfoController = new JoinInfoController(inclusionFilters)
    val coefficientController = new CoefficientController
    val consumptionController = new ConsumptionController
    val dimensionalCoherenceController = new DimensionalCoherenceController
    val joinAnagraficaConsumptionController = new JoinAnagraficaConsumptionController
    val classiGruppiDiMisuraPortataRcugas = new ClassiGruppiDiMisuraPortataRcugas
    val incoerentiGdMController = new IncoerentiGdMController
    val esclusiController = new EsclusiController
    val pprofInfoController = new PprofInfoController
    val carriBombolaiExclusionController = new CarriBombolaiExclusionController

    // Inizializzazione dei DAO per le tabelle rimanenti (alcune di esse in scrittura)
    val monthTreatmentDAO = new MonthTreatmentDAO
    val validatedFlowDAO = new ValidatedFlowDAO
    val tabProfiliGiorniStdPercDao = new TabProfiliGiorniStdPercDao
    val segmentDAO = new SegmentDAO
    val dailyConsumptionDAO = new DailyConsumptionDAO
    val dailyConsumptionEsclusiDAO = new DailyConsumptionEsclusiDAO
    val dailyConsumptionIncGdMDAO = new  DailyConsumptionIncGdMDAO
    val tdgCoeffKDao = new TdgCoeffKDao
    val carriBombolaiFileDao = new CarriBombolaiFileDao

    strongExclusionFilterController.backupExclusionFolder()

    // Lettura delle misure RML
    val rmlMeasures = flowController.getRmlMeasures(startFlowDate, endFlowDate, treatmentCalcMode == TreatmentCalcMode.rcugas)
    // Selezione dell'ultima RML a parità di PdR e data
    val (treatmentRml, mot6Rml) = flowController.getLatestRml(rmlMeasures)
    // Lettura delle misure di trattamento
    val treatmentMeasures = flowController.getTreatmentMeasures(startFlowDate, endFlowDate, treatmentCalcMode == TreatmentCalcMode.rcugas)
      .union(treatmentRml).coalesce(Environment.getNumberPartition.toInt)
    // Lettura delle misure con motivazione 6
    val mot6Measures = flowController.getMot6Measures(startFlowDate, endFlowDate, treatmentCalcMode == TreatmentCalcMode.rcugas)
      .union(mot6Rml).coalesce(Environment.getNumberPartition.toInt)
    // Lettura delle misure di intervento/cambio contatore Im1 e Igmg
    val im1IgmgMeasure = flowController.getIm1IgmgMeasures(startFlowDate, endFlowDate, treatmentCalcMode == TreatmentCalcMode.rcugas)
    // Lettura di tutte le altre le misure
    val otherMeasures = flowController.getAllOtherMeasures(startFlowDate, endFlowDate, treatmentCalcMode == TreatmentCalcMode.rcugas)

    // Lettura delle tabelle da rcugas
    val rcugasConnessioniDistr2 = rcuGasConnessioniDistr2DAO.get(startFlowDate, endFlowDate)
    val suspendedPdr = rcuSuspendedPdrDAO.get()
    val rcuGasTech = rcuGasVarMisuratorePDAO.get(startFlowDate, endFlowDate)
    val rcuGasVarConvertitore = rcuGasVarConvertitorePDAO.get(startFlowDate, endFlowDate)
    val rcuGasVarPrelAnnuoP = rcuGasVarPrelAnnuoPDAO.get(startFlowDate, endFlowDate)
    val rcuGasVarProfiloP = rcuGasVarProfiloPDAO.get(startFlowDate, endFlowDate)
    val rcugasMassivoP = rcuGasMassivoPDAO.get() //.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val tdgCoeffKDf = tdgCoeffKDao.get(rcugasMassivoP, startFlowDate, endFlowDate)

    // Annullamento dei flussi (flussi di rettifica con motivazione 3 o 6)
    val filteredTreatmentMeasures = cancelController.cancelTreatmentMeasures(treatmentMeasures)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val filteredMot6Measures = cancelController.cancelMot6(mot6Measures)
    val filteredOtherMeasures = cancelController.cancelOtherMeasures(otherMeasures)
    val im1IgmgMeasuresFiltered = cancelController.cancelIgmrAndIgmgWithIgmrMot3Measures(im1IgmgMeasure)
    // Rettifica dei flussi Im1/Igmg da parte dei flussi RML con motivazione 1 o 2
    val correctedIm1IgmgAndTreatment = im1IgmgCorrectionController.getAdjustedIgmg(im1IgmgMeasuresFiltered.union(filteredTreatmentMeasures).coalesce(Environment.getNumberPartition.toInt))

    // Calcolo del trattamento e scrittura della tabella monthtreatment
    val rcuTreatment = rcuGasVarTrattamentoPDAO.get(rcugasMassivoP, startFlowDate, endFlowDate, treatmentCalcMode)
    val monthWithTreatment = treatmentController.calc(filteredTreatmentMeasures, startFlowDate, endFlowDate, treatmentCalcMode, rcuTreatment)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    monthTreatmentDAO.writeParquet(monthWithTreatment, startPeriodDate, endPeriodDate)

    // Unione delle misure ottenute dalle precedenti procedure (lettura, filtraggio, rimozione duplicati, annullamenti, rettifiche)
    val measures = filteredMot6Measures.union(filteredOtherMeasures)
      .union(correctedIm1IgmgAndTreatment)
      .coalesce(Environment.getNumberPartition.toInt)

    // Applicazione delle priorità tra flussi
    val priorityMeasures = priorityController.getPriorityMeasures(measures)

    // Associazione delle informazioni da rcugas alle misure
    val measuresWithInformation = joinInfoController.get(priorityMeasures, monthWithTreatment, rcugasMassivoP,
      rcugasConnessioniDistr2, suspendedPdr, rcuGasTech, rcuGasVarPrelAnnuoP, rcuGasVarProfiloP, rcuGasVarConvertitore,
      startPeriodDate, endPeriodDate)

    // Esclusione di alcune misure utilizzando il codice remi (deve essere effettuato in questo momento poiché prima non erano disponibili le informazioni sul codice remi)
    val measuresWithInfoFiltered = softExclusionFilterController.excludeRemiPdr(measuresWithInformation, Environment.getRcugasSqoopDate)
    val measuresWithInfo = strongExclusionFilterController.excludeRemiPdr(measuresWithInfoFiltered, Environment.getRcugasSqoopDate)

    // Assegnazione a ogni misura di
    // - coefficiente di correzione, utilizzato nella formula 3 del calcolo dei consumi
    // - tipo dimensionale, che determinerà il campo (misura o convertito) utilizzato per il calcolo dei consumi
    // Successivamente, viene scritta la tabella validated_flows, contenente i flussi che concorrono al calcolo
    val measureWithInfoValued = coefficientController.get(measuresWithInfo)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    // measureWithInfoValued.checkpoint()
    validatedFlowDAO.writeParquet(measureWithInfoValued)

    //calcolo trattamento RCUGAS per successiva colonna trattamento in pubblicazione
    val rcuTreatmentRcu = rcuGasVarTrattamentoPDAO.get(rcugasMassivoP, startFlowDate, endFlowDate, TreatmentCalcMode.rcugas)

    // tripla pdr, annomese, trattamento rcugas
    val rcuTreatmentRcuFiltered = treatmentController.getRcuTreatmentWithoutMeasures(rcuTreatmentRcu, startFlowDate, endFlowDate)
      .map(f => (f._1._1, f._1._2, f._2.toString))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    tdgCoeffKDf.persist(StorageLevel.MEMORY_AND_DISK_SER)

    filteredTreatmentMeasures.unpersist(true)
    rcugasMassivoP.unpersist(true)

    // Lettura di una mappa statica che contiene la relazione tra classe misuratore e portata massima.
    /** Deprecated, vedere [[classiGruppiDiMisuraPortataRcugas.get]]. */
    val classiGruppiDiMisuraPortataRcugasGet = Environment.getSpark.sparkContext.broadcast(classiGruppiDiMisuraPortataRcugas.get.collectAsMap().toMap)

    // Creazione dei segmenti (misura sx, misura dx) per la successiva parte di calcolo dei consumi
    val segmentsRDD = consumptionController.calcCouple(measureWithInfoValued)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    segmentDAO.writeParquet(segmentsRDD.mapValues({ case (couples, externalInfos) => couples }))

    measureWithInfoValued.unpersist(true)

    // Verifica della coerenza del tipo dimensionale all'interno dei segmenti
    val coherentSegmentsRdd = dimensionalCoherenceController.getCoherentSegmentsRDD(segmentsRDD)

    // Calcolo dei consumi giornalieri
    val consumptions = consumptionController.calcDailyConsumptions(
      coherentSegmentsRdd
      , classiGruppiDiMisuraPortataRcugasGet
      , sterilizeIsEnable = Environment.getSterilizeMeasureEnable.equals("true")
    )
    //.repartition(Environment.getNumberPartition.toInt * 6)

    // consumptions.checkpoint()

    // Associazione ai PdR di alcune info da anagrafica (e.g. piva_udb, piva_distr, ...) e crea la tabella daily_consumption
    val dailyConsumptionsWithInfo = joinAnagraficaConsumptionController.getJoinedAnagrafica(consumptions, startPeriodDate, endPeriodDate, session, monthWithTreatment, Environment.getRcugasSqoopDate)

    // Applicazione di eslcusioni forzate, se presenti, e scrittura della daily consumption
    val dailyConsumptionsForceExclusion = softForceExclusionFilterController.forceExclusion(dailyConsumptionsWithInfo, Environment.getRcugasSqoopDate)
    val dailyConsumptions = strongForceExclusionFilterController.forceExclusion(dailyConsumptionsForceExclusion, Environment.getRcugasSqoopDate)

    //Si effettua l'aggiunta della colonna Treatment_pub, relativa ai pdr che presentano trattamento="N", per cui si effettua un nuovo controllo sulla RCUGAS
    val rcuTreatmentRcuUnique = Environment.getSpark.sqlContext.createDataFrame(rcuTreatmentRcuFiltered)
      .withColumnRenamed("_1", "pdrT")
      .withColumnRenamed("_2", "annoMeseT")
      .withColumnRenamed("_3", "treatmentRcu")

    val dailyConsumption = Environment.getSpark.sqlContext.createDataFrame(dailyConsumptions.keys)

    val joinedDailyConsumption = dailyConsumption
      .join(rcuTreatmentRcuUnique.filter(col("treatmentRcu")=!="N"),
          col(DailyConsumptionAGGSBGSchema.pdr).equalTo(col("pdrT")) and col(DailyConsumptionAGGSBGSchema.annoMese).equalTo(col("annoMeseT")),
        "left")
      .withColumn(DailyConsumptionAGGSBGSchema.treatmentPub, when(
        col(DailyConsumptionAGGSBGSchema.treatment) === "N",
        lit(col("treatmentRcu"))
      ).otherwise(lit(null))
      )

    dailyConsumptionDAO.writeTemporaryTable1(joinedDailyConsumption)

    dailyConsumption.unpersist(blocking = true)
    joinedDailyConsumption.unpersist(blocking = true)
    rcuTreatmentRcuUnique.unpersist(blocking = true)
    rcuTreatmentRcu.unpersist(blocking = true)
    segmentsRDD.unpersist(blocking = true)
    monthWithTreatment.unpersist(blocking = true)
    dailyConsumptionsForceExclusion.unpersist(blocking = true)
    dailyConsumptions.unpersist(blocking = true)
    dailyConsumptionsWithInfo.unpersist(blocking = true)
    consumptions.unpersist(blocking = true)
    coherentSegmentsRdd.unpersist(blocking = true)
    segmentsRDD.unpersist(blocking = true)
    measuresWithInfo.unpersist(blocking = true)
    measuresWithInfoFiltered.unpersist(blocking = true)
    measuresWithInformation.unpersist(blocking = true)
    priorityMeasures.unpersist(blocking = true)
    measures.unpersist(blocking = true)
    rcuTreatment.unpersist(blocking = true)
    correctedIm1IgmgAndTreatment.unpersist(blocking = true)
    filteredOtherMeasures.unpersist(blocking = true)
    filteredTreatmentMeasures.unpersist(blocking = true)
    filteredMot6Measures.unpersist(blocking = true)
    rcugasConnessioniDistr2.unpersist(blocking = true)
    suspendedPdr.unpersist(blocking = true)
    rcuGasTech.unpersist(blocking = true)
    rcuGasVarPrelAnnuoP.unpersist(blocking = true)
    rcuGasVarProfiloP.unpersist(blocking = true)
    otherMeasures.unpersist(blocking = true)
    im1IgmgMeasure.unpersist(blocking = true)
    im1IgmgMeasuresFiltered.unpersist(blocking = true)
    mot6Measures.unpersist(blocking = true)
    treatmentMeasures.unpersist(blocking = true)
    rmlMeasures.unpersist(blocking = true)

    val dailyCWithInfoTmp1 = dailyConsumptionDAO.readTmp1Table(Environment.executionId)
    val carriBombolaiExclusionDataset = carriBombolaiFileDao.get
    val (tabProfBeforeRemiInMonths, tabProfAfterRemiInMonths, tabProfBeforeRemiOutMonths, tabProfAfterRemiOutMonths)
    = tabProfiliGiorniStdPercDao.get(startFlowDate, endFlowDate, startPeriodDate, endPeriodDate)
    val (tabProfiliGiorniStdPercOutOfMonthPre, tabProfiliGiorniStdPercOutOfMonthPost) = pprofInfoController.getOutMonth(dailyCWithInfoTmp1, tabProfBeforeRemiOutMonths, tabProfAfterRemiOutMonths)
    val dailyWithPprofs = pprofInfoController.get(dailyCWithInfoTmp1, tabProfBeforeRemiInMonths, tabProfAfterRemiInMonths, tabProfiliGiorniStdPercOutOfMonthPre, tabProfiliGiorniStdPercOutOfMonthPost)
    val dailyWithF3 = pprofInfoController.adjustF3Calculation(dailyWithPprofs)
    val dailyF2 = pprofInfoController.adjustF2Calculation(dailyWithF3)
    val dailyWithoutCarriBombolai = carriBombolaiExclusionController.excludeCarriBombolai(dailyF2, carriBombolaiExclusionDataset)

    dailyConsumptionDAO.writeParquet(dailyWithoutCarriBombolai)

    dailyWithoutCarriBombolai.unpersist(true)
    dailyCWithInfoTmp1.unpersist(true)
    tabProfBeforeRemiInMonths.unpersist(true)
    tabProfAfterRemiInMonths.unpersist(true)
    tabProfBeforeRemiOutMonths.unpersist(true)
    tabProfAfterRemiOutMonths.unpersist(true)
    tabProfiliGiorniStdPercOutOfMonthPre.unpersist(true)
    tabProfiliGiorniStdPercOutOfMonthPost.unpersist(true)
    dailyWithPprofs.unpersist(true)
    dailyF2.unpersist(true)
    dailyWithF3.unpersist(blocking = true)

    val dailyConsumptionFromHive = dailyConsumptionDAO.readPartition(Environment.executionId)

    val dailyConsumptionsIncoerentiGdM = incoerentiGdMController.getIncoerentiGdM(dailyConsumptionFromHive, tdgCoeffKDf)
    dailyConsumptionIncGdMDAO.writeParquet(dailyConsumptionsIncoerentiGdM.coalesce(200))
    val dailyConsumptionsEsclusi = esclusiController.getExcludedPdrs(dailyConsumptionFromHive)
    dailyConsumptionEsclusiDAO.writeParquet(dailyConsumptionsEsclusi.coalesce(200))

    dailyConsumptionDAO.deleteTemporaryTables()

  }
}
