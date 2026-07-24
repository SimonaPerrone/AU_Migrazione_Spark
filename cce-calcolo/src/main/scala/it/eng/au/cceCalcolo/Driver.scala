package it.eng.au.cceCalcolo

import it.eng.au.cceCalcolo.args.{ArgsFactory, CalcoloArgs}
import it.eng.au.cceCalcolo.controller.{AmmissibilitaCheckController, AnnullamentoMisureController, CalcoloController}
import it.eng.au.cceCalcolo.dao.ammissibilita.{ReportAmmissibilitaFileDAO, ReportAmmissibilitaPodDAO}
import it.eng.au.cceCalcolo.dao.annullamenti.{CceMoAnnullate1gDAO, CceMoAnnullate1gEinDAO, CceMoAnnullate2gDAO, CceMoAnnullate2gEinDAO}
import it.eng.au.cceCalcolo.dao.calcoloOutput.{CceCalcTrackDAO, CceCalcoloAnagraficaDAO, CceCalcoloPDAO, CceCalcoloPRDAO, CceCalcoloPReinDAO, CceCalcoloPeinDAO, CceCalcoloTrattamentoDAO}
import it.eng.au.cceCalcolo.dao.flussiMisure.{FlussoMisureEstensioneQuartiDAO, FlussoMisureEstensioneQuartiInDAO, FlussoMisureQuartiDAO, FlussoMisureQuartiInDAO}
import it.eng.au.cceCalcolo.dao.ghigliottina.GhigliottinaDAO
import it.eng.au.cceCalcolo.dao.rcu.{RcuAziendaPDAO, RcuFornituraPDAO, RcuPodDistrPDAO, RcuPodMisurePDAO, RcuPodPDAO, RcuPodTecnPDAO, RcuPodUddPDAO, RcuTariffaPDAO, RcuUddPDAO}
import it.eng.au.cceCalcolo.dao.rcus.{RcusFornituraPDAO, RcusPodMisurePDAO, RcusPodPDAO, RcusPodUddPDAO}
import it.eng.au.cceCalcolo.model.CceCalcTrackModel
import it.eng.au.cceCalcolo.utility.ApplicationConstant
import it.eng.au.cceCalcolo.utility.ApplicationConstant.{APPLICATION_NAME, LOG_NAME}
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.log.LogUtility

import java.time.format.DateTimeFormatter
import org.apache.log4j.Logger

object Driver {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      Environment.getOrCreate(APPLICATION_NAME, LOG_NAME)

      implicit val parsedArgs: CalcoloArgs = ArgsFactory.parse(args)
      implicit val executionId: Long = Environment.startDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toLong

      LogUtility.printInitialLog()

      run(parsedArgs)

      LogUtility.printFinalLog()
    }
    catch {
      case e: Throwable =>
        logger.error(s"An error occurred in the procedure.")
        throw e
    }
  }

  def run(implicit args: CalcoloArgs): Unit = {
    // args
    val annoIn = if (args.annoCalc.isEmpty) Environment.startDateTime.minusYears(1).getYear else args.annoCalc.get
    val meseIn = if (args.meseCalc.isEmpty) Environment.startDateTime.minusMonths(1).getMonthValue else args.meseCalc.get
    val tipoCalcIn = args.tipoCalc.get
    val massivoFlag = if (args.massivoFlag.isEmpty) false else args.massivoFlag.get

    // Inizializzazione dei DAO per le tabelle in rcu e rcus (contenente i dati di anagrafica)
    val rcuAziendaPDAO = new RcuAziendaPDAO
    val rcuFornituraPDAO = new RcuFornituraPDAO
    val rcuPodDistrPDAO = new RcuPodDistrPDAO
    val rcuPodMisurePDAO = new RcuPodMisurePDAO
    val rcuPodPDao = new RcuPodPDAO
    val rcuPodTecnPDAO = new RcuPodTecnPDAO
    val rcuPodUddPDAO = new RcuPodUddPDAO
    val rcuTariffaPDAO = new RcuTariffaPDAO
    val rcuUddPDAO = new RcuUddPDAO

    val rcusFornituraPDAO = new RcusFornituraPDAO
    val rcusPodMisurePDAO = new RcusPodMisurePDAO
    val rcusPodPDAO = new RcusPodPDAO
    val rcusPodUddPDAO = new RcusPodUddPDAO

    // Inizializzazione dei DAO per le tabelle relative ai flussi misure
    val flussoMisureQuartiDAO = new FlussoMisureQuartiDAO
    val flussoMisureEstensioneQuartiDAO = new FlussoMisureEstensioneQuartiDAO
    val flussoMisureQuartiInDAO = new FlussoMisureQuartiInDAO
    val flussoMisureEstensioneQuartiInDAO = new FlussoMisureEstensioneQuartiInDAO

    //Inizializzazione DAO report ammissibilità
    val reportAmmissibilitaPodDAO = new ReportAmmissibilitaPodDAO
    val reportAmmissibilitaFileDAO = new ReportAmmissibilitaFileDAO

    // Inizializzazione DAO tabella ghigliottina
    val ghigliottinaDAO = new GhigliottinaDAO

    //Inizializzazione controller
    val annullamentoMisureController = new AnnullamentoMisureController
    val calcoloController = new CalcoloController
    val ammissibilitaCheckController = new AmmissibilitaCheckController

    // Inizializzazione DAO tabelle output del processo di annullamento
    val cceMoAnnullate1gDAO = new CceMoAnnullate1gDAO
    val cceMoAnnullate2gDAO = new CceMoAnnullate2gDAO
    val cceMoAnnullate1gEinDAO = new CceMoAnnullate1gEinDAO
    val cceMoAnnullate2gEinDAO = new CceMoAnnullate2gEinDAO

    // Inizializzazione DAO tabelle output del processo di calcolo
    val cceCalcoloAnagraficaDAO = new CceCalcoloAnagraficaDAO
    val cceCalcoloPDAO = new CceCalcoloPDAO
    val cceCalcoloPeinDAO = new CceCalcoloPeinDAO
    val cceCalcoloPRDAO = new CceCalcoloPRDAO
    val cceCalcoloPReinDAO = new CceCalcoloPReinDAO
    val cceCalcoloTrattamentoDAO = new CceCalcoloTrattamentoDAO
    val cceCalcTrackDAO = new CceCalcTrackDAO


    // Lettura delle tabelle
    //rcu
    val rcuAziendaP = rcuAziendaPDAO.getRcuAziendaP
    val rcuFornituraP = rcuFornituraPDAO.getRcuFornituraP
    val rcuPodDistrP = rcuPodDistrPDAO.getRcuPodDistrP
    val rcuPodMisureP = rcuPodMisurePDAO.getRcuPodMisureP(annoIn, meseIn, args.meseCalc)
    val rcuPodP = rcuPodPDao.getRcuPodP
    val rcuPodTecnP = rcuPodTecnPDAO.getRcuPodTecnP
    val rcuPodUddP = rcuPodUddPDAO.getRcuPodUddP
    val rcuTariffaP = rcuTariffaPDAO.getRcuTariffaP
    val rcuUddP = rcuUddPDAO.getRcuUddP
    //rcus
    val rcusFornituraP = rcusFornituraPDAO.getRcusFornituraP
    val rcusPodMisureP = rcusPodMisurePDAO.getRcusPodMisureP(annoIn, meseIn, args.meseCalc)
    val rcusPodP = rcusPodPDAO.getRcusPodP
    val rcusPodUddP = rcusPodUddPDAO.getRcusPodUddP

    //ghigliottina
    val dataGhigliottina = ghigliottinaDAO.getDataGhigliottina

    //calc track attuale
    val cceCalcTrackDF = cceCalcTrackDAO.getCceCalcTrack

    //report ammissibilità
    val reportPod = reportAmmissibilitaPodDAO.getReportPod
    val reportFile = reportAmmissibilitaFileDAO.getReportFile
    val ammissibilitaDF = ammissibilitaCheckController.getReportAmmissibilitaUnion(reportPod, reportFile)

    (tipoCalcIn, massivoFlag, args.meseCalc) match {
      case (ApplicationConstant.calcoloP, _, _) =>
        val monthly = true
        val flussoMisureQuarti = flussoMisureQuartiDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuarti = flussoMisureEstensioneQuartiDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gDF = annullamentoMisureController.get1G2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)
        val cceMoAnnullate2gDF = annullamentoMisureController.get2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)

        cceMoAnnullate1gDAO.writeOnHive(cceMoAnnullate1gDF)

        cceMoAnnullate1gDF.unpersist(blocking = true)

        val cceMoAnnullate1gDF1 = cceMoAnnullate1gDAO.getMoAnnullate

        cceMoAnnullate2gDF.cache

        val cceCalcoloPDF = calcoloController.getPAggregation(flussoMisureQuarti, dataGhigliottina, cceMoAnnullate1gDF1, cceMoAnnullate2gDF, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
            Seq(CceCalcTrackModel(ApplicationConstant.calcoloP, "M", annoIn.toString, meseIn.toString, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gDAO.writeOnHive(cceMoAnnullate2gDF)
        cceCalcoloPDAO.writeOnHive(cceCalcoloPDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPR, true, Some(meseIn)) =>
        val monthly = true
        val flussoMisureQuarti = flussoMisureQuartiDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuarti = flussoMisureEstensioneQuartiDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gDF = annullamentoMisureController.get1G2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)
        val cceMoAnnullate2gDF = annullamentoMisureController.get2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)

        cceMoAnnullate1gDAO.writeOnHive(cceMoAnnullate1gDF)

        cceMoAnnullate1gDF.unpersist(blocking = true)

        val cceMoAnnullate1gDF1 = cceMoAnnullate1gDAO.getMoAnnullate

        cceMoAnnullate2gDF.cache

        val cceCalcoloPRMassivoDF = calcoloController.getPRMassivoAggregation(flussoMisureQuarti, cceMoAnnullate1gDF1, cceMoAnnullate2gDF, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
            Seq(CceCalcTrackModel(ApplicationConstant.calcoloPR, "M", annoIn.toString, meseIn.toString, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gDAO.writeOnHive(cceMoAnnullate2gDF)
        cceCalcoloPRDAO.writeOnHive(cceCalcoloPRMassivoDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPR, true, None) =>
        val monthly = false
        val flussoMisureQuarti = flussoMisureQuartiDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuarti = flussoMisureEstensioneQuartiDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gDF = annullamentoMisureController.get1G2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)
        val cceMoAnnullate2gDF = annullamentoMisureController.get2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)

        cceMoAnnullate1gDAO.writeOnHive(cceMoAnnullate1gDF)

        cceMoAnnullate1gDF.unpersist(blocking = true)

        val cceMoAnnullate1gDF1 = cceMoAnnullate1gDAO.getMoAnnullate

        cceMoAnnullate2gDF.cache

        val cceCalcoloPRMassivoDF = calcoloController.getPRMassivoAggregation(flussoMisureQuarti, cceMoAnnullate1gDF1, cceMoAnnullate2gDF, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
          Seq(CceCalcTrackModel(ApplicationConstant.calcoloPR, "M", annoIn.toString, null, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gDAO.writeOnHive(cceMoAnnullate2gDF)
        cceCalcoloPRDAO.writeOnHive(cceCalcoloPRMassivoDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPR, false, None) =>
        val monthly = false
        val flussoMisureQuarti = flussoMisureQuartiDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuarti = flussoMisureEstensioneQuartiDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gDF = annullamentoMisureController.get1G2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)
        val cceMoAnnullate2gDF = annullamentoMisureController.get2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)

        cceMoAnnullate1gDAO.writeOnHive(cceMoAnnullate1gDF)

        cceMoAnnullate1gDF.unpersist(blocking = true)

        val cceMoAnnullate1gDF1 = cceMoAnnullate1gDAO.getMoAnnullate

        cceMoAnnullate2gDF.cache

        val cceCalcoloPRIncrementaleDF = calcoloController.getPRIncrementaleAggregation(flussoMisureQuarti, cceMoAnnullate1gDF1, cceMoAnnullate2gDF, cceCalcTrackDF, annoIn, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
          Seq(CceCalcTrackModel(ApplicationConstant.calcoloPR, "I", annoIn.toString, null, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gDAO.writeOnHive(cceMoAnnullate2gDF)
        cceCalcoloPRDAO.writeOnHive(cceCalcoloPRIncrementaleDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPR, false, Some(meseIn)) =>
        val monthly = true
        val flussoMisureQuarti = flussoMisureQuartiDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuarti = flussoMisureEstensioneQuartiDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gDF = annullamentoMisureController.get1G2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)
        val cceMoAnnullate2gDF = annullamentoMisureController.get2G(flussoMisureEstensioneQuarti, flussoMisureQuarti)

        cceMoAnnullate1gDAO.writeOnHive(cceMoAnnullate1gDF)

        cceMoAnnullate1gDF.unpersist(blocking = true)

        val cceMoAnnullate1gDF1 = cceMoAnnullate1gDAO.getMoAnnullate

        cceMoAnnullate2gDF.cache

        val cceCalcoloPRIncrementaleDF = calcoloController.getPRIncrementaleAggregation(flussoMisureQuarti, cceMoAnnullate1gDF1, cceMoAnnullate2gDF, cceCalcTrackDF, annoIn, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
            Seq(CceCalcTrackModel(ApplicationConstant.calcoloPR, "I", annoIn.toString, meseIn.toString, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gDAO.writeOnHive(cceMoAnnullate2gDF)
        cceCalcoloPRDAO.writeOnHive(cceCalcoloPRIncrementaleDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPein, _, _) =>
        val monthly = true
        val flussoMisureQuartiIn = flussoMisureQuartiInDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuartiIn = flussoMisureEstensioneQuartiInDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gEinDF = annullamentoMisureController.get1G2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)
        val cceMoAnnullate2gEinDF = annullamentoMisureController.get2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)

        cceMoAnnullate1gEinDAO.writeOnHive(cceMoAnnullate1gEinDF)

        cceMoAnnullate1gEinDF.unpersist(blocking = true)

        val cceMoAnnullate1gEinDF1 = cceMoAnnullate1gEinDAO.getMoAnnullate

        cceMoAnnullate2gEinDF.cache

        val cceCalcoloPeinDF = calcoloController.getPeinAggregation(flussoMisureQuartiIn, dataGhigliottina, cceMoAnnullate1gEinDF1, cceMoAnnullate2gEinDF, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
            Seq(CceCalcTrackModel(ApplicationConstant.calcoloPein, "M", annoIn.toString, meseIn.toString, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gEinDAO.writeOnHive(cceMoAnnullate2gEinDF)
        cceCalcoloPeinDAO.writeOnHive(cceCalcoloPeinDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPRein, true, Some(meseIn)) =>
        val monthly = true
        val flussoMisureQuartiIn = flussoMisureQuartiInDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuartiIn = flussoMisureEstensioneQuartiInDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gEinDF = annullamentoMisureController.get1G2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)
        val cceMoAnnullate2gEinDF = annullamentoMisureController.get2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)

        cceMoAnnullate1gEinDAO.writeOnHive(cceMoAnnullate1gEinDF)

        cceMoAnnullate1gEinDF.unpersist(blocking = true)

        val cceMoAnnullate1gEinDF1 = cceMoAnnullate1gEinDAO.getMoAnnullate

        cceMoAnnullate2gEinDF.cache

        val cceCalcoloPReinMassivoDF = calcoloController.getPReinMassivoAggregation(flussoMisureQuartiIn, cceMoAnnullate1gEinDF1, cceMoAnnullate2gEinDF, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
            Seq(CceCalcTrackModel(ApplicationConstant.calcoloPRein, "M", annoIn.toString, meseIn.toString, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gEinDAO.writeOnHive(cceMoAnnullate2gEinDF)
        cceCalcoloPReinDAO.writeOnHive(cceCalcoloPReinMassivoDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPRein, true, None) =>
        val monthly = false
        val flussoMisureQuartiIn = flussoMisureQuartiInDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuartiIn = flussoMisureEstensioneQuartiInDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gEinDF = annullamentoMisureController.get1G2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)
        val cceMoAnnullate2gEinDF = annullamentoMisureController.get2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)

        cceMoAnnullate1gEinDAO.writeOnHive(cceMoAnnullate1gEinDF)

        cceMoAnnullate1gEinDF.unpersist(blocking = true)

        val cceMoAnnullate1gEinDF1 = cceMoAnnullate1gEinDAO.getMoAnnullate

        cceMoAnnullate2gEinDF.cache

        val cceCalcoloPReinMassivoDF = calcoloController.getPReinMassivoAggregation(flussoMisureQuartiIn, cceMoAnnullate1gEinDF1, cceMoAnnullate2gEinDF, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
          Seq(CceCalcTrackModel(ApplicationConstant.calcoloPRein, "M", annoIn.toString, null, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gEinDAO.writeOnHive(cceMoAnnullate2gEinDF)
        cceCalcoloPReinDAO.writeOnHive(cceCalcoloPReinMassivoDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPRein, false, Some(meseIn)) =>
        val monthly = true
        val flussoMisureQuartiIn = flussoMisureQuartiInDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuartiIn = flussoMisureEstensioneQuartiInDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gEinDF = annullamentoMisureController.get1G2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)
        val cceMoAnnullate2gEinDF = annullamentoMisureController.get2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)

        cceMoAnnullate1gEinDAO.writeOnHive(cceMoAnnullate1gEinDF)

        cceMoAnnullate1gEinDF.unpersist(blocking = true)

        val cceMoAnnullate1gEinDF1 = cceMoAnnullate1gEinDAO.getMoAnnullate

        cceMoAnnullate2gEinDF.cache

        val cceCacoloPReinIncrementaleDF = calcoloController.getPReinIncrementaleAggregation(flussoMisureQuartiIn, cceMoAnnullate1gEinDF1, cceMoAnnullate2gEinDF, cceCalcTrackDF, annoIn, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
            Seq(CceCalcTrackModel(ApplicationConstant.calcoloPRein, "I", annoIn.toString, meseIn.toString, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
    )

        cceMoAnnullate2gEinDAO.writeOnHive(cceMoAnnullate2gEinDF)
        cceCalcoloPReinDAO.writeOnHive(cceCacoloPReinIncrementaleDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (ApplicationConstant.calcoloPRein, false, None) =>
        val monthly = false
        val flussoMisureQuartiIn = flussoMisureQuartiInDAO.get(annoIn, meseIn, monthly)
        val flussoMisureEstensioneQuartiIn = flussoMisureEstensioneQuartiInDAO.get(annoIn, meseIn, monthly)

        val cceMoAnnullate1gEinDF = annullamentoMisureController.get1G2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)
        val cceMoAnnullate2gEinDF = annullamentoMisureController.get2Gein(flussoMisureEstensioneQuartiIn, flussoMisureQuartiIn)

        cceMoAnnullate1gEinDAO.writeOnHive(cceMoAnnullate1gEinDF)

        cceMoAnnullate1gEinDF.unpersist(blocking = true)

        val cceMoAnnullate1gEinDF1 = cceMoAnnullate1gEinDAO.getMoAnnullate

        cceMoAnnullate2gEinDF.cache

        val cceCacoloPReinIncrementaleDF = calcoloController.getPReinIncrementaleAggregation(flussoMisureQuartiIn, cceMoAnnullate1gEinDF1, cceMoAnnullate2gEinDF, cceCalcTrackDF, annoIn, ammissibilitaDF)

        val cceCalcTrackDeltaDF = Environment.spark.createDataFrame(
          Seq(CceCalcTrackModel(ApplicationConstant.calcoloPRein, "I", annoIn.toString, null, Environment.startDateTime.toString, ApplicationConstant.esitoPositivoCalcolo))
        )

        cceMoAnnullate2gEinDAO.writeOnHive(cceMoAnnullate2gEinDF)
        cceCalcoloPReinDAO.writeOnHive(cceCacoloPReinIncrementaleDF)
        cceCalcTrackDAO.writeCalcTrack(cceCalcTrackDeltaDF)

      case (_, _, _) => print("No case match found")
    }

    val cceCalcoloAnagraficaDF = calcoloController.getAnagrafica(rcuFornituraP, rcusFornituraP, rcuTariffaP, rcuPodDistrP, rcuAziendaP, rcuPodTecnP, rcuPodUddP, rcusPodUddP, rcuUddP, rcuPodP, rcusPodP)
    val cceCalcoloTrattamentoDF =
      if (args.meseCalc.isEmpty)
        calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 1)
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 2))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 3))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 4))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 5))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 6))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 7))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 8))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 9))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 10))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 11))
          .unionByName(calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, 12))
      else
        calcoloController.getTrattamento(rcuPodP, rcusPodP, rcuPodMisureP, rcusPodMisureP, annoIn, meseIn)

    //scrittura output calcolo anagrafica e trattamento
    cceCalcoloAnagraficaDAO.writeOnHive(cceCalcoloAnagraficaDF)
    cceCalcoloTrattamentoDAO.writeOnHive(cceCalcoloTrattamentoDF)
  }


}
