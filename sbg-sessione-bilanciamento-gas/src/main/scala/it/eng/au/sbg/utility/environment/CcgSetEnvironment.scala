package it.eng.au.sbg.utility.environment

import it.eng.au.aggiustamentoGas.utility.args.FlowArgsConfig
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.{CCG, PROPERTIES_YEAR_MOTNHS_DAYS_FORMAT, PROPERTIES_YEAR_MOTNHS_FORMAT}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.log.LogUtility
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.convertDateTimeInStringWithFormat
import org.apache.log4j.Logger
import org.joda.time.DateTime

object CcgSetEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    val applicationName = FieldConstants.CCG_SBG_APPLICATION_NAME
    val logName = FieldConstants.CCG_SBG_LOG
    logger.warn(s"$logName Start $applicationName")

    Environment.getOrCreate(applicationName, logName, flowArgsConfig.pathToProperties)
    LogUtility.printVersionInfo()

    logger.warn(s"${FieldConstants.CCG_SBG_LOG} Run CCG Sessione Bilanciamento Gas")

    val sysDate = flowArgsConfig.dateToRun.getOrElse(new DateTime(Environment.executionId))
    set(sysDate)
  }

  def set(sysDate: DateTime): Unit = {

    val periodReadStartDate = sysDate.minus(Environment.getPeriodStartDateTimeBack)

    val periodReadEndDate = periodReadStartDate

    val flowReadStartDate = sysDate.minus(Environment.getFlowStartDateTimeBack)

    val flowReadEndDate = sysDate

    val flowReadGhigliottina = sysDate.minus(Environment.getFlowGhigliottinaTimeBack)

    val rcugasSqoopDate = flowReadGhigliottina

    val periodCompetence = periodReadStartDate

    val session = CCG

    Environment.setProperty("period.read.startDate", convertDateTimeInStringWithFormat(periodReadStartDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("period.read.endDate", convertDateTimeInStringWithFormat(periodReadEndDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("flow.read.startDate", convertDateTimeInStringWithFormat(flowReadStartDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("flow.read.endDate", convertDateTimeInStringWithFormat(flowReadEndDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("flow.read.ghigliottina", convertDateTimeInStringWithFormat(flowReadGhigliottina, PROPERTIES_YEAR_MOTNHS_DAYS_FORMAT))
    Environment.setProperty("rcugas.sqoop.date", convertDateTimeInStringWithFormat(rcugasSqoopDate, PROPERTIES_YEAR_MOTNHS_DAYS_FORMAT))
    Environment.setProperty("period.competence", convertDateTimeInStringWithFormat(periodCompetence, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("session", session)
  }
}
