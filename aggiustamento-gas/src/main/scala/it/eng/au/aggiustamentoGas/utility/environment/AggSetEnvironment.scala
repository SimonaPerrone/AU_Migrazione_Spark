package it.eng.au.aggiustamentoGas.utility.environment

import it.eng.au.aggiustamentoGas.utility.args.FlowArgsConfig
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants
import it.eng.au.aggiustamentoGas.utility.log.LogUtility
import org.apache.log4j.Logger

object AggSetEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  /**
   * Inizializza Spark con il metodo [[Environment.getOrCreate]] ed effettua la print delle info del sofwtare
   * @param flowArgsConfig gli argomenti post-parsing
   */
  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    val applicationName = FieldConstants.AGG_APPLICATION_NAME
    val logName = FieldConstants.AGG_LOG
    logger.warn(s"$logName Start $applicationName")

    Environment.getOrCreate(applicationName, logName, flowArgsConfig.pathToProperties)
    LogUtility.printVersionInfo()

    if (flowArgsConfig.dateToRun.isDefined) logger.warn(s"${FieldConstants.AGG_LOG} The date passed as argument ${flowArgsConfig.dateToRun.get} is ignored")
  }
}
