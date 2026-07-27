package it.sferanet.au.utilities.environment

import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utilities.args.FlowArgsConfig
import org.apache.log4j.Logger

object CaEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"${Constants.CA_LOG} Start CAGas")

    Environment.getOrCreate("[CDP] Calcolo Consumo Annuo Gas", flowArgsConfig.pathToProperties)
    Environment.setProperty("sessione", "CDP")

    if (flowArgsConfig.dateToRun.isDefined) logger.warn(s"${Constants.CA_LOG} The date ${flowArgsConfig.dateToRun.get} passed as argument is ignored")
    logger.warn(s"${Constants.CA_LOG} Run Calcolo Consumo Annuo Gas")
    logger.warn(s"${Constants.CA_LOG} Properties:")
    logger.warn(s"${Constants.CA_LOG} ${Environment.printProperties}")
    logger.warn(s"${Constants.CA_LOG} Execution ID: ${Environment.executionId}")
    logger.warn(s"${Constants.CA_LOG} Date: ${Environment.getPartitionDate}")
    logger.warn(s"${Constants.CA_LOG} applicationID=${Environment.getSpark.sparkContext.applicationId}")
  }
}
