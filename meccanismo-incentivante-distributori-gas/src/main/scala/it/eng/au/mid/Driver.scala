package it.eng.au.mid

import it.eng.au.mid.args.{Args, ArgsFactory}
import it.eng.au.mid.common.VersionLoggingUtility.printVersionInfo
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.calcolo.{AggBackInTimeFlow, AggStandardFlow, SbgStandardFlow}
import it.eng.au.mid.flow.pubblicazione.{PredisposizioneMid1Flow, PredisposizioneMid2Flow, PubblicazioneMid1Flow, PubblicazioneMid2Flow}
import org.apache.log4j.Logger

import java.time.LocalDateTime

object Driver {
  @transient private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val jobName = "MID Gas"
    logger.warn(s"Inizio processo $jobName")
    val currentDateTime = LocalDateTime.now()
    printVersionInfo()

    logger.warn(s"Argomenti passati: ${args.mkString(",")}")
    logger.warn(s"Lettura argomenti")
    val flowArgsConfig = ArgsFactory.parse(args)

    logger.warn(s"Creazione contesto Spark")
    Environment.getOrCreate(
      appName = s"MID_${flowArgsConfig.flow}_$currentDateTime",
      path = flowArgsConfig.pathToProperties)

    logger.warn(s"Proprietà:")
    logger.warn(Environment.printProperties)

    logger.warn(s"Avvio processo")
    flowArgsConfig.flow match {
      // calcolo MID
      case Args.SBG_FLOW => new SbgStandardFlow().run()
      case Args.AGG_FLOW => new AggStandardFlow().run()
      case Args.AGG_BIT_FLOW => new AggBackInTimeFlow().run()
      // preparazione pubblicazione
      case Args.MID1_PREP => new PredisposizioneMid1Flow().run()
      case Args.MID2_PREP => new PredisposizioneMid2Flow().run()
      // pubblicazione file zip
      case Args.MID1_PUBB => new PubblicazioneMid1Flow().run()
      case Args.MID2_PUBB => new PubblicazioneMid2Flow().run()
      // else
      case x => throw new Exception(s"Flusso $x non configurato")
    }

    logger.warn(s"Fine processo $jobName")
  }
}
