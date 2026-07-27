package it.eng.au.freezerPreCalcolo.utility.environment

import it.eng.au.freezerPreCalcolo.utility.Constants
import it.eng.au.freezerPreCalcolo.utility.args.FlowArgsConfig
import org.apache.log4j.Logger

object CdpEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"${Constants.CDP_LOG} Start Freezer procedure")

    Environment.getOrCreate("[CDP] Freezer", flowArgsConfig.pathToProperties)
    Environment.setProperty("session", "CDP")

    if (flowArgsConfig.dateToRun.isDefined) logger.warn(s"${Constants.CDP_LOG} The date ${flowArgsConfig.dateToRun.get} passed as argument is ignored")
    logger.warn(s"${Constants.CDP_LOG} Run Freezer")
    logger.warn(s"${Constants.CDP_LOG} Properties:")
    logger.warn(s"${Constants.CDP_LOG} ${Environment.printProperties}")
    logger.warn(s"${Constants.CDP_LOG} Execution ID: ${Environment.executionId}")
    logger.warn(s"${Constants.CDP_LOG} Date: ${Environment.getPartitionDate}")
    logger.warn(s"${Constants.CDP_LOG} applicationID=${Environment.getSpark.sparkContext.applicationId}")
  }
}
