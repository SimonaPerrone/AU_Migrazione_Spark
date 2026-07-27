package it.eng.au.ccgPubblicazione.controller.sessionfactory.impl

import it.eng.au.ccgPubblicazione.args.FlowArgsConfig
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.cdpric.{CdpRicGestoreAggregato, CdpRicIdAggregato, CdpRicUdbAggregato, CdpRicUddAggregato}
import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.RunnableAggregator
import it.eng.au.ccgPubblicazione.controller.sessionfactory.traits.CdpFinRicFlow
import it.eng.au.ccgPubblicazione.dao.cdp.{CaFinalAggRicDao, ValidatedFlowsCdpDao}
import it.eng.au.ccgPubblicazione.utility.Constants.{CDP_RIC, CDP_RIC_LOG}
import it.eng.au.ccgPubblicazione.utility.{Environment, VersionLoggingUtility}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp
import java.time.LocalDateTime

object CdpRicSession extends CdpFinRicFlow {
  override val session: String = CDP_RIC
  override val sessionLog: String = CDP_RIC_LOG
  override val idPdrElencoFlussi: RunnableAggregator = CdpRicIdAggregato
  override val uddPdrElencoFlussi: RunnableAggregator = CdpRicUddAggregato
  override val udbPdrElencoFlussi: RunnableAggregator = CdpRicUdbAggregato
  override val gestorePdrElencoFlussi: RunnableAggregator = CdpRicGestoreAggregato
  override val idPdrElencoFlussiIncoerentiAB: RunnableAggregator = null
  override val uddPdrElencoFlussiIncoerentiAB: RunnableAggregator = null
  override val udbPdrElencoFlussiIncoerentiAB: RunnableAggregator = null
  override val gestorePdrElencoFlussiIncoerentiAB: RunnableAggregator = null
  override val idPdrElencoFlussiIncoerentiC: RunnableAggregator = null
  override val uddPdrElencoFlussiIncoerentiC: RunnableAggregator = null
  override val udbPdrElencoFlussiIncoerentiC: RunnableAggregator = null
  override val gestorePdrElencoFlussiIncoerentiC: RunnableAggregator = null
  override val idPdrElencoFlussiEsclusi: RunnableAggregator = null
  override val uddPdrElencoFlussiEsclusi: RunnableAggregator = null
  override val udbPdrElencoFlussiEsclusi: RunnableAggregator = null
  override val gestorePdrElencoFlussiEsclusi: RunnableAggregator = null

  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"$sessionLog Start Pubblicazione CCG - CDP RIC")
    VersionLoggingUtility.printVersionInfo()

    Environment.getOrCreate("Pubblicazione Contatore Consumi Gas - CDP RIC", flowArgsConfig.pathToProperties)
    Environment.setProperty("daterun",  Timestamp.valueOf(LocalDateTime.now()).toString)

    logger.warn(s"$sessionLog Run Pubblicazione CCG - CDP RIC")

    logger.warn(s"$sessionLog Properties:")
//    logger.warn(s"$sessionLog ${Environment.printProperties}")
    //    logger.warn(s"$sessionLog Execution ID: ${Environment.executionId}")
    //    logger.warn(s"$sessionLog Date: ${Environment.getPartitionDate}")
    logger.warn(s"$sessionLog Date to run: ${Environment.getProperty("daterun")}")
    logger.warn(s"$sessionLog applicationID=${Environment.spark.sparkContext.applicationId}")

  }

  override def readConsumptionWithLastPartition: (DataFrame, String) = {
    CaFinalAggRicDao.readLastPartition
  }

  override def readValidation(partition: String): DataFrame = {
    ValidatedFlowsCdpDao.readPartition(partition)
  }
}
