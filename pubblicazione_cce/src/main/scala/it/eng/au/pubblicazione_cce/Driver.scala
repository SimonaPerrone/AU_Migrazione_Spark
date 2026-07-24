package it.eng.au.pubblicazione_cce

import it.eng.au.pubblicazione_cce.args.ArgsFactory
import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.dao.cce.{CceCalcoloPDao, CceCalcoloPRDao, CceCalcoloPReinDao, CceCalcoloPeinDao}
import it.eng.au.pubblicazione_cce.flow.{PubblicazioneCAFlow, PubblicazioneFlow}
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import it.eng.au.pubblicazione_cce.utility.file.FileUtility
import it.eng.au.pubblicazione_cce.utility.log.LoggingUtility.printVersionInfo
import org.apache.log4j.Logger

import java.io.File
import java.time.LocalDateTime
import scala.reflect.io.Directory

object Driver {
  @transient private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val jobName = "Pubblicazione_CCE"
    logger.warn(s"Inizio processo $jobName")
    val currentDateTime = LocalDateTime.now()
    printVersionInfo()

    logger.warn(s"Argomenti passati: ${args.mkString(",")}")
    logger.warn(s"Lettura argomenti")
    val flowArgsConfig = ArgsFactory.parse(args)

    logger.warn(s"Creazione contesto Spark")
    Environment.getOrCreate(
      appName = s"${jobName}_${flowArgsConfig.flow}_$currentDateTime",
      path = flowArgsConfig.pathToProperties)

    logger.warn(s"Proprietà:")
    logger.warn(Environment.printProperties)

    logger.warn(s"Avvio processo")

    //val dataRichieste = flowArgsConfig.
    //new PubblicazioneFlow()
    // import dati oracle

    val dataRichieste = flowArgsConfig.dataRichieste
    val processo = flowArgsConfig.flow

    try {
      processo match {
        case CostantiCCE.PROCESSO_P =>
          new PubblicazioneFlow(processo = processo, dataRichieste = dataRichieste, misureDao = new CceCalcoloPDao)
            .run()
        case CostantiCCE.PROCESSO_PR =>
          new PubblicazioneFlow(processo = processo, dataRichieste = dataRichieste, misureDao = new CceCalcoloPRDao)
            .run()
        case CostantiCCE.PROCESSO_PEIN_UPPER =>
          new PubblicazioneFlow(processo = processo.toUpperCase(), dataRichieste = dataRichieste, misureDao = new CceCalcoloPeinDao)
            .run()
        case CostantiCCE.PROCESSO_PREIN_UPPER =>
          new PubblicazioneFlow(processo = processo.toUpperCase(), dataRichieste = dataRichieste, misureDao = new CceCalcoloPReinDao)
            .run()
        case CostantiCCE.PROCESSO_CA =>  new PubblicazioneCAFlow(processo = processo.toUpperCase(), dataRichieste = dataRichieste)
          .run()
        case _ => throw new Exception("Errore in input flow: " + processo)
      }
    } finally {
      //cancella file in cartelle temporanee: decommentare se richiesto
      // val tmpOutDir = Environment.getOutputFileTemporaryPath
      // logger.warn(s"Cancellazione file temporanei in: $tmpOutDir")
      // FileUtility.deleteContents(new File(tmpOutDir))
    }

    logger.warn(s"Fine processo $jobName")
  }
}
