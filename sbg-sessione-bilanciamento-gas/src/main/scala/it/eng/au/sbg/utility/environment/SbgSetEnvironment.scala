package it.eng.au.sbg.utility.environment

import it.eng.au.aggiustamentoGas.utility.args.FlowArgsConfig
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.log.LogUtility
import org.apache.log4j.Logger

object SbgSetEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    val applicationName = FieldConstants.SBG_APPLICATION_NAME
    val logName = FieldConstants.SBG_LOG
    logger.warn(s"$logName Start $applicationName")

    Environment.getOrCreate(applicationName, logName, flowArgsConfig.pathToProperties)
    LogUtility.printVersionInfo()

    if (flowArgsConfig.dateToRun.isDefined) logger.warn(s"${FieldConstants.SBG_LOG} The date passed as argument ${flowArgsConfig.dateToRun.get} is ignored")
  }
}
