package it.eng.au.ccgPubblicazione.controller.sessionfactory.impl

import it.eng.au.ccgPubblicazione.args.FlowArgsConfig
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.cdpfin.{CdpFinGestoreAggregato, CdpFinIdAggregato, CdpFinUdbAggregato, CdpFinUddAggregato}
import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.RunnableAggregator
import it.eng.au.ccgPubblicazione.controller.sessionfactory.traits.CdpFinRicFlow
import it.eng.au.ccgPubblicazione.dao.cdp.{CaFinalAggFinDao, ValidatedFlowsCdpDao}
import it.eng.au.ccgPubblicazione.utility.Constants.{CDP_FIN, CDP_FIN_LOG}
import it.eng.au.ccgPubblicazione.utility.{Environment, VersionLoggingUtility}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp
import java.time.LocalDateTime

object CdpFinSession extends CdpFinRicFlow {
  override val session: String = CDP_FIN
  override val sessionLog: String = CDP_FIN_LOG
  override val idPdrElencoFlussi: RunnableAggregator = CdpFinIdAggregato
  override val uddPdrElencoFlussi: RunnableAggregator = CdpFinUddAggregato
  override val udbPdrElencoFlussi: RunnableAggregator = CdpFinUdbAggregato
  override val gestorePdrElencoFlussi: RunnableAggregator = CdpFinGestoreAggregato
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
    logger.warn(s"$sessionLog Start Pubblicazione CCG - CDP FIN")
    VersionLoggingUtility.printVersionInfo()

    Environment.getOrCreate("Pubblicazione Contatore Consumi Gas - CDP FIN", flowArgsConfig.pathToProperties)
    Environment.setProperty("daterun",  Timestamp.valueOf(LocalDateTime.now()).toString)

    logger.warn(s"$sessionLog Run Pubblicazione CCG - CDP FIN")

    logger.warn(s"$sessionLog Properties:")
//    logger.warn(s"$sessionLog ${Environment.printProperties}")
    //    logger.warn(s"$sessionLog Execution ID: ${Environment.executionId}")
    //    logger.warn(s"$sessionLog Date: ${Environment.getPartitionDate}")
    logger.warn(s"$sessionLog Date to run: ${Environment.getProperty("daterun")}")
    logger.warn(s"$sessionLog applicationID=${Environment.spark.sparkContext.applicationId}")

  }

  override def readConsumptionWithLastPartition: (DataFrame, String) = {
    CaFinalAggFinDao.readLastPartition
  }

  override def readValidation(partition: String): DataFrame = {
    ValidatedFlowsCdpDao.readPartition(partition)
  }
}
