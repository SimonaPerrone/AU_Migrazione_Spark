package it.eng.au.sgsFlussoStoricoGas.run.pubblicazione

import it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.{ValidationController, ZipWriterController}
import it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udb.{UdbSwgAPublishController, UdbSwgSPublishController, UdbUigAPublishController, UdbUigSPublishController, UdbVtgAPublishController, UdbVtgSPublishController}
import it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni.udd.{UddSwgAPublishController, UddSwgSPublishController, UddUigAPublishController, UddUigSPublishController, UddVtgAPublishController, UddVtgSPublishController}
import it.eng.au.sgsFlussoStoricoGas.dao.aggregazione.AggregatoreInfoDettDao
import it.eng.au.sgsFlussoStoricoGas.dao.executionTrack.SgsExecutionTrackDao
import it.eng.au.sgsFlussoStoricoGas.dao.perimetro.{SgsPerimetroSwgADao, SgsPerimetroSwgSDao, SgsPerimetroUigADao, SgsPerimetroUigSDao, SgsPerimetroVtgSDao}
import it.eng.au.sgsFlussoStoricoGas.dao.pubblicazioni.{PubblicazioneInfoDao, PubblicazioneInfoDettDao, SgsReportDao}
import it.eng.au.sgsFlussoStoricoGas.utility.args.FlowArgsFactory
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import it.eng.au.sgsFlussoStoricoGas.utility.log.LogUtility
import org.apache.log4j.Logger

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    try {

      val parsedArgs = FlowArgsFactory.parse(args)
      val applicationName = "SGS - Flusso Storico Gas - Pubblicazione"
      val logName = "SGS LOG:"

      Environment.getOrCreate(applicationName, logName, parsedArgs.pathToProperties)

      LogUtility.printInitialLog()

      runPubblicazione()

      LogUtility.printFinalLog()

    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }

  private def runPubblicazione(): Unit = {

    //Params to run
    val giornoEx = Environment.startDateTime.getDayOfMonth.toString
    val giornoIn = Environment.getProperty("execution.day")
    val giornoInVtgA = Environment.getProperty("execution.day.vtga")
    val activationFlow = if (giornoEx.equals(giornoIn)) true else false
    val activationFlowVtgA = if(giornoEx.equals(giornoInVtgA)) true else false
    // Dichiarazione come lazy val
    @transient lazy val xsdPath: String = Environment.getXsdPath
    @transient lazy val outputPathPrefix: String = Environment.getOutputPathXml
    @transient lazy val pdrPerFile: Int = Environment.getPdrPerFile.toInt

    val boolUdbPubblicazioneSwgS = Environment.getBoolPubblicazioneUdbSwgS.toBoolean
    val boolUdbPubblicazioneSwgA = Environment.getBoolPubblicazioneUdbSwgA.toBoolean
    val boolUdbPubblicazioneUigS = Environment.getBoolPubblicazioneUdbUigS.toBoolean
    val boolUdbPubblicazioneUigA = Environment.getBoolPubblicazioneUdbUigA.toBoolean
    val boolUdbPubblicazioneVtgS = Environment.getBoolPubblicazioneUdbVtgS.toBoolean
    val boolUdbPubblicazioneVtgA = Environment.getBoolPubblicazioneUdbVtgA.toBoolean
    val boolUddPubblicazioneSwgS = Environment.getBoolPubblicazioneUddSwgS.toBoolean
    val boolUddPubblicazioneSwgA = Environment.getBoolPubblicazioneUddSwgA.toBoolean
    val boolUddPubblicazioneUigS = Environment.getBoolPubblicazioneUddUigS.toBoolean
    val boolUddPubblicazioneUigA = Environment.getBoolPubblicazioneUddUigA.toBoolean
    val boolUddPubblicazioneVtgS = Environment.getBoolPubblicazioneUddVtgS.toBoolean
    val boolUddPubblicazioneVtgA = Environment.getBoolPubblicazioneUddVtgA.toBoolean

    //Dao
    val aggregatoreInfoDettDao = new AggregatoreInfoDettDao
    val executionTrackDao = new SgsExecutionTrackDao
    val perimetroSwgSDao = new SgsPerimetroSwgSDao
    val perimetroSwgADao = new SgsPerimetroSwgADao
    val perimetroUigSDao = new SgsPerimetroUigSDao
    val perimetroUigADao = new SgsPerimetroUigADao
    val perimetroVtgSDao = new SgsPerimetroVtgSDao
    val pubblicazioneInfoDao = new PubblicazioneInfoDao
    val pubblicazioneInfoDettDao = new PubblicazioneInfoDettDao
    val sgsReportDao = new SgsReportDao

    //Controller
    //UDB
    val udbSwgSPublishController = new UdbSwgSPublishController
    val udbSwgAPublishController = new UdbSwgAPublishController
    val udbUigSPublishController = new UdbUigSPublishController
    val udbUigAPublishController = new UdbUigAPublishController
    val udbVtgSPublishController = new UdbVtgSPublishController
    val udbVtgAPublishController = new UdbVtgAPublishController
    //UDD
    val uddSwgSPublishController = new UddSwgSPublishController
    val uddSwgAPublishController = new UddSwgAPublishController
    val uddUigSPublishController = new UddUigSPublishController
    val uddUigAPublishController = new UddUigAPublishController
    val uddVtgSPublishController = new UddVtgSPublishController
    val uddVtgAPublishController = new UddVtgAPublishController

    val validationController = new ValidationController
    val zipWriterController = new ZipWriterController


    //UDB
    if (boolUdbPubblicazioneSwgS && activationFlow) {
      val perimetroSwgSDF = perimetroSwgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = udbSwgSPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = udbSwgSPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = udbSwgSPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroSwgSDF, pubListValidated)
      val pubblicazioneInfoDettDF = udbSwgSPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroSwgSDF, pubListValidated)
      val sgsReportDF = udbSwgSPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUdbPubblicazioneSwgA && activationFlow) {
      val perimetroSwgADF = perimetroSwgADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = udbSwgAPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = udbSwgAPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = udbSwgAPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroSwgADF, pubListValidated)
      val pubblicazioneInfoDettDF = udbSwgAPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroSwgADF, pubListValidated)
      val sgsReportDF = udbSwgAPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUdbPubblicazioneUigS && activationFlow) {
      val perimetroUigSDF = perimetroUigSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = udbUigSPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = udbUigSPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = udbUigSPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroUigSDF, pubListValidated)
      val pubblicazioneInfoDettDF = udbUigSPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroUigSDF, pubListValidated)
      val sgsReportDF = udbUigSPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUdbPubblicazioneUigA && activationFlow) {
      val perimetroUigADF = perimetroUigADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = udbUigAPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = udbUigAPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = udbUigAPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroUigADF, pubListValidated)
      val pubblicazioneInfoDettDF = udbUigAPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroUigADF, pubListValidated)
      val sgsReportDF = udbUigAPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
  }

    if (boolUdbPubblicazioneVtgS) {
      val perimetroVtgSDF = perimetroVtgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = udbVtgSPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = udbVtgSPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = udbVtgSPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroVtgSDF, pubListValidated)
      val pubblicazioneInfoDettDF = udbVtgSPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroVtgSDF, pubListValidated)
      val sgsReportDF = udbVtgSPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUdbPubblicazioneVtgA && activationFlowVtgA) {
      val perimetroVtgADF = perimetroVtgSDao.readLastForVtgAAggr(executionTrackDao.getExecIdForVtgAAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = udbVtgAPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = udbVtgAPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = udbVtgAPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroVtgADF, pubListValidated)
      val pubblicazioneInfoDettDF = udbVtgAPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroVtgADF, pubListValidated)
      val sgsReportDF = udbVtgAPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    //UDD
    if (boolUddPubblicazioneSwgS && activationFlow) {
      val perimetroSwgSDF = perimetroSwgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = uddSwgSPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = uddSwgSPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = uddSwgSPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroSwgSDF, pubListValidated)
      val pubblicazioneInfoDettDF = uddSwgSPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroSwgSDF, pubListValidated)
      val sgsReportDF = uddSwgSPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUddPubblicazioneSwgA && activationFlow) {
      val perimetroSwgADF = perimetroSwgADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = uddSwgAPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = uddSwgAPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = uddSwgAPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroSwgADF, pubListValidated)
      val pubblicazioneInfoDettDF = uddSwgAPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroSwgADF, pubListValidated)
      val sgsReportDF = uddSwgAPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUddPubblicazioneUigS && activationFlow) {
      val perimetroUigSDF = perimetroUigSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = uddUigSPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = uddUigSPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = uddUigSPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroUigSDF, pubListValidated)
      val pubblicazioneInfoDettDF = uddUigSPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroUigSDF, pubListValidated)
      val sgsReportDF = uddUigSPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUddPubblicazioneUigA && activationFlow) {
      val perimetroUigADF = perimetroUigADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = uddUigAPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = uddUigAPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = uddUigAPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroUigADF, pubListValidated)
      val pubblicazioneInfoDettDF = uddUigAPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroUigADF, pubListValidated)
      val sgsReportDF = uddUigAPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUddPubblicazioneVtgS) {
      val perimetroVtgSDF = perimetroVtgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = uddVtgSPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = uddVtgSPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = uddVtgSPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroVtgSDF, pubListValidated)
      val pubblicazioneInfoDettDF = uddVtgSPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroVtgSDF, pubListValidated)
      val sgsReportDF = uddVtgSPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

    if (boolUddPubblicazioneVtgA && activationFlowVtgA) {
      val perimetroVtgADF = perimetroVtgSDao.readLastForVtgAAggr(executionTrackDao.getExecIdForVtgAAggregation)
      val aggregatoreInfoDettDF = aggregatoreInfoDettDao.readLastPartition(executionTrackDao.getExecIdForPublish)
      val xmlOutputDF = uddVtgAPublishController.getXmlOutputDF(aggregatoreInfoDettDF)
      val pubList = uddVtgAPublishController.writeXml(xmlOutputDF, outputPathPrefix, pdrPerFile)
      val validator = validationController.createValidator(xsdPath)
      val pubListValidated = validationController.validateXml(validator, pubList)
      val pubblicazioneInfoDF = uddVtgAPublishController.getPubblicazioneInfo(aggregatoreInfoDettDF, perimetroVtgADF, pubListValidated)
      val pubblicazioneInfoDettDF = uddVtgAPublishController.getPubblicazioneInfoDett(aggregatoreInfoDettDF, perimetroVtgADF, pubListValidated)
      val sgsReportDF = uddVtgAPublishController.getSgsReportDF(aggregatoreInfoDettDF, pubblicazioneInfoDF, pubblicazioneInfoDettDF)
      zipWriterController.writeZip(pubListValidated)
      pubblicazioneInfoDao.writeParquet(pubblicazioneInfoDF)
      pubblicazioneInfoDettDao.writeParquet(pubblicazioneInfoDettDF)
      sgsReportDao.writeParquet(sgsReportDF)
      aggregatoreInfoDettDF.unpersist(blocking = true)
      xmlOutputDF.unpersist(blocking = true)
      pubblicazioneInfoDF.unpersist(blocking = true)
      pubblicazioneInfoDettDF.unpersist(blocking = true)
    }

  }

}