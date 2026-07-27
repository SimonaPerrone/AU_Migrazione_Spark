package it.eng.au.sbg

import it.eng.au.aggiustamentoGas.controller._
import it.eng.au.aggiustamentoGas.controller.classeGdM.ClassiGruppiDiMisuraPortataRcugas
import it.eng.au.aggiustamentoGas.dao.agg.{MonthTreatmentDAO, SegmentDAO, ValidatedFlowDAO}
import it.eng.au.aggiustamentoGas.dao.rcugas._
import it.eng.au.aggiustamentoGas.dao.settlegas.TabProfiliGiorniStdPercDao
import it.eng.au.aggiustamentoGas.filter.exclusion.{ExclusionFilterController, ForceExclusionController}
import it.eng.au.aggiustamentoGas.filter.inclusion.InclusionFilterController
import it.eng.au.aggiustamentoGas.utility.args.FlowArgsFactory
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.CCG
import it.eng.au.aggiustamentoGas.utility.constants.TreatmentCalcMode
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.log.LogUtility
import it.eng.au.sbg.controller._
import it.eng.au.sbg.dao.rcugas.RcuGasVarTrattamentoPDAOSbg
import it.eng.au.sbg.dao.sbg.DailyConsumptionDAOSbg
import it.eng.au.sbg.utility.environment.{CcgSetEnvironment, SbgSetEnvironment}
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

object Driver {
  @transient private lazy val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val flowArgsConfig = FlowArgsFactory.parse(args)
      if (flowArgsConfig.session.contains(CCG))
        CcgSetEnvironment.setEnvironment(flowArgsConfig)
      else
        SbgSetEnvironment.setEnvironment(flowArgsConfig)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()
    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }

  def run(): Unit = {
    val startFlowDate = Environment.getFlowStartDate
    val endFlowDate = Environment.getFlowEndDate

    val startPeriodDate = Environment.getPeriodStartDate
    val endPeriodDate = Environment.getPeriodEndDate

    val treatmentCalcMode = TreatmentCalcMode.rcugas

    val session = Environment.getSession

    val rcuGasMassivoPDAO = new RcuGasMassivoPDAO
    val rcuGasConnessioniDistr2DAO = new RcuGasConnessioniDistr2DAO
    val rcuSuspendedPdrDAO = new SuspendedPdrDAO
    val rcuGasVarMisuratorePDAO = new RcuGasVarMisuratorePDAO
    val rcuGasVarPrelAnnuoPDAO = new RcuGasVarPrelAnnuoPDAO
    val rcuGasVarProfiloPDAO = new RcuGasVarProfiloPDAO
    val rcuGasVarTrattamentoPDAO = new RcuGasVarTrattamentoPDAOSbg

    val inclusionFilters = InclusionFilterController.getFilter(rcuGasMassivoPDAO.readParquet, rcuGasConnessioniDistr2DAO.readParquet)
    val softExclusionFilterController = new ExclusionFilterController(isStrong = false)
    val strongExclusionFilterController = new ExclusionFilterController(isStrong = true)
    val softForceExclusionFilterController = new ForceExclusionController(isStrong = false)
    val strongForceExclusionFilterController = new ForceExclusionController(isStrong = false)
    val flowController = new FlowControllerSbg(softExclusionFilterController, strongExclusionFilterController, inclusionFilters)
    val cancelController = new CancelController
    val treatmentController = new TreatmentControllerSbg
    val priorityController = new PriorityController
    val im1IgmgCorrectionController = new Im1IgmgCorrectionController
    val joinInfoController = new JoinInfoControllerSbg(inclusionFilters)
    val coefficientController = new CoefficientController
    val consumptionController = new ConsumptionControllerSbg
    val dimensionalCoherenceController = new DimensionalCoherenceController
    val joinAnagraficaConsumptionController = new JoinAnagraficaConsumptionController
    val classiGruppiDiMisuraPortataRcugas = new ClassiGruppiDiMisuraPortataRcugas

    val monthTreatmentDAO = new MonthTreatmentDAO
    val validatedFlowDAO = new ValidatedFlowDAO
    val tabProfiliGiorniStdPercDao = new TabProfiliGiorniStdPercDao
    val segmentDAO = new SegmentDAO
    val dailyConsumptionDAO = new DailyConsumptionDAOSbg

    strongExclusionFilterController.backupExclusionFolder()

    val rcugasMassivoP = rcuGasMassivoPDAO.get() //.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val rcuTreatment = rcuGasVarTrattamentoPDAO.get(rcugasMassivoP, startFlowDate, endFlowDate, treatmentCalcMode).persist(StorageLevel.MEMORY_AND_DISK)
    val monthWithTreatment = treatmentController.calc(rcuTreatment, startFlowDate, endFlowDate)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val rcuTreatmentYDf = rcuGasVarTrattamentoPDAO.getTrattamentoYDf(monthWithTreatment, startPeriodDate).persist(StorageLevel.MEMORY_AND_DISK)

    val rmlMeasures = flowController.getRmlMeasures(startFlowDate, endFlowDate, rcuTreatment = rcuTreatmentYDf)
    val (treatmentRml, mot6Rml) = flowController.getLatestRml(rmlMeasures)
    val treatmentMeasures = flowController.getTreatmentMeasures(startFlowDate, endFlowDate, rcuTreatment = rcuTreatmentYDf)
      .union(treatmentRml).coalesce(Environment.getNumberPartition.toInt)
    val mot6Measures = flowController.getMot6Measures(startFlowDate, endFlowDate, rcuTreatment = rcuTreatmentYDf)
      .union(mot6Rml).coalesce(Environment.getNumberPartition.toInt)
    val im1IgmgMeasure = flowController.getIm1IgmgMeasures(startFlowDate, endFlowDate, rcuTreatment = rcuTreatmentYDf)
    val otherMeasures = flowController.getAllOtherMeasures(startFlowDate, endFlowDate, rcuTreatment = rcuTreatmentYDf)

    val rcugasConnessioniDistr2 = rcuGasConnessioniDistr2DAO.get(startFlowDate, endFlowDate)
    val suspendedPdr = rcuSuspendedPdrDAO.get()
    val rcuGasTech = rcuGasVarMisuratorePDAO.get(startFlowDate, endFlowDate)
    val rcuGasVarPrelAnnuoP = rcuGasVarPrelAnnuoPDAO.get(startFlowDate, endFlowDate)
    val rcuGasVarProfiloP = rcuGasVarProfiloPDAO.get(startFlowDate, endFlowDate)

    val filteredTreatmentMeasures = cancelController.cancelTreatmentMeasures(treatmentMeasures).persist(StorageLevel.MEMORY_AND_DISK)
    val filteredMot6Measures = cancelController.cancelMot6(mot6Measures)
    val filteredOtherMeasures = cancelController.cancelOtherMeasures(otherMeasures)

    val correctedIm1IgmgAndTreatment = im1IgmgCorrectionController.getAdjustedIgmg(im1IgmgMeasure.union(filteredTreatmentMeasures).coalesce(Environment.getNumberPartition.toInt))



    monthTreatmentDAO.writeParquet(monthWithTreatment, startPeriodDate, endPeriodDate)

    val measures = filteredMot6Measures.union(filteredOtherMeasures)
      .union(correctedIm1IgmgAndTreatment)
      .coalesce(Environment.getNumberPartition.toInt)

    val priorityMeasures = priorityController.getPriorityMeasures(measures)

    val measuresWithInformation = joinInfoController.get(priorityMeasures, monthWithTreatment, rcugasMassivoP,
      rcugasConnessioniDistr2, suspendedPdr, rcuGasTech, rcuGasVarPrelAnnuoP, rcuGasVarProfiloP,
      startPeriodDate, endPeriodDate)

    // Since we didn't have cod_remi before, we run the remi exclusion at this point
    val measuresWithInfoFiltered = softExclusionFilterController.excludeRemiPdr(measuresWithInformation, Environment.getRcugasSqoopDate)
    val measuresWithInfo = strongExclusionFilterController.excludeRemiPdr(measuresWithInfoFiltered, Environment.getRcugasSqoopDate)

    val measureWithInfoValued = coefficientController.get(measuresWithInfo)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    validatedFlowDAO.writeParquet(measureWithInfoValued)

    filteredTreatmentMeasures.unpersist(true)

    val tabProfiliGiorniStdPercBCMap = Environment.getSpark.sparkContext.broadcast(
      tabProfiliGiorniStdPercDao.get(startFlowDate, endFlowDate).keyBy(record => (record.data, record.prof, record.idRegClim)).mapValues(_.pprofkPercentage).collectAsMap().toMap
    )

    val classiGruppiDiMisuraPortataRcugasGet = Environment.getSpark.sparkContext.broadcast(classiGruppiDiMisuraPortataRcugas.get.collectAsMap().toMap)

    val segmentsRDD = consumptionController.calcCouple(measureWithInfoValued)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    segmentDAO.writeParquet(segmentsRDD.mapValues({ case (couples, externalInfos) => couples }))

    measureWithInfoValued.unpersist(true)

    val coherentSegmentsRdd = dimensionalCoherenceController.getCoherentSegmentsRDD(segmentsRDD)

    val consumptions = consumptionController.calcDailyConsumptions(
      coherentSegmentsRdd
      , tabProfiliGiorniStdPercBCMap
      , classiGruppiDiMisuraPortataRcugasGet
      , sterilizeIsEnable = Environment.getSterilizeMeasureEnable.equals("true")
    )


    val dailyConsumptionsWithInfo = joinAnagraficaConsumptionController.getJoinedAnagrafica(consumptions, startPeriodDate, endPeriodDate, session, monthWithTreatment, Environment.getRcugasSqoopDate)

    val dailyConsumptionsForceExclusion = softForceExclusionFilterController.forceExclusion(dailyConsumptionsWithInfo, Environment.getRcugasSqoopDate)
    val dailyConsumptions = strongForceExclusionFilterController.forceExclusion(dailyConsumptionsForceExclusion, Environment.getRcugasSqoopDate)
    dailyConsumptionDAO.writeParquet(dailyConsumptions.keys)
  }
}
