package it.eng.au.portaleConsumi.utility.environment

import it.eng.au.portaleConsumi.utility.args.PortaleConsumiArgs
import it.eng.au.portaleConsumi.utility.common.VersionLoggingUtility
import org.apache.log4j.Logger

object PortaleConsumiEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: PortaleConsumiArgs): Unit = {
    VersionLoggingUtility.printVersionInfo()
    Environment.getOrCreate("Portale Consumi", flowArgsConfig.pathToProperties)

    logger.warn(s"Properties:")
    logger.warn(s"${Environment.printProperties}")
    logger.warn(s"Execution ID: ${Environment.executionId}")
    logger.warn(s"applicationID=${Environment.getSpark.sparkContext.applicationId}")
  }

}
