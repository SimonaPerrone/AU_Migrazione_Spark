package it.eng.au.aggiustamentoGas.utility.environment

import it.eng.au.aggiustamentoGas.utility.args.FlowArgsConfig
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.{CCG, PROPERTIES_YEAR_MOTNHS_DAYS_FORMAT, PROPERTIES_YEAR_MOTNHS_FORMAT}
import it.eng.au.aggiustamentoGas.utility.log.LogUtility
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.convertDateTimeInStringWithFormat
import org.apache.log4j.Logger
import org.joda.time.DateTime

object CcgSetEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  /**
   * Inizializza Spark con il metodo [[Environment.getOrCreate]], e setta le properties in base alla data fornita in input al processo
   * @param flowArgsConfig gli argomenti post-parsing
   */
  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    val applicationName = FieldConstants.CCG_AGG_APPLICATION_NAME
    val logName = FieldConstants.CCG_AGG_LOG
    logger.warn(s"$logName Start $applicationName")

    Environment.getOrCreate(applicationName, logName, flowArgsConfig.pathToProperties)
    LogUtility.printVersionInfo()

    /** Prendiamo la data se passata come input, oppure consideriamo la data odierna */
    val sysDate = flowArgsConfig.dateToRun.getOrElse(new DateTime(Environment.executionId))
    set(sysDate)
  }

  /**
   * Crea tutti i parametri relativi alle date (periodo di lettura delle misure, periodo di calcolo dei consumi, ...) in base al parametro `sysDate`,
   * e setta questi stessi nelle properties in lettura al processo.
   * @param sysDate data fornita come input o, se non presente, la data di oggi
   */
  def set(sysDate: DateTime): Unit = {
    val lowerBoundPeriodReadStart = DateTime.parse("2020-01-01")
    val periodReadStartDateTimeBack = sysDate.withMonthOfYear(1).minus(Environment.getPeriodStartDateTimeBack)
    val periodReadStartDate = if (periodReadStartDateTimeBack.isBefore(lowerBoundPeriodReadStart)) lowerBoundPeriodReadStart else periodReadStartDateTimeBack

    val periodReadEndDate = sysDate.minus(Environment.getPeriodEndDateTimeBack).dayOfMonth.withMaximumValue

    val flowReadStartDate = periodReadStartDate.minus(Environment.getFlowStartDateTimeBackWrtPeriod)

    val flowReadEndDate = sysDate.minus(Environment.getFlowEndDateTimeBack)

    val flowReadGhigliottina = sysDate.minus(Environment.getFlowGhigliottinaTimeBack)

    val rcugasSqoopDate = sysDate.minus(Environment.getRcugasSqoopDateTimeBack)

    val session = CCG

    Environment.setProperty("period.read.startDate", convertDateTimeInStringWithFormat(periodReadStartDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("period.read.endDate", convertDateTimeInStringWithFormat(periodReadEndDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("flow.read.startDate", convertDateTimeInStringWithFormat(flowReadStartDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("flow.read.endDate", convertDateTimeInStringWithFormat(flowReadEndDate, PROPERTIES_YEAR_MOTNHS_FORMAT))
    Environment.setProperty("flow.read.ghigliottina", convertDateTimeInStringWithFormat(flowReadGhigliottina, PROPERTIES_YEAR_MOTNHS_DAYS_FORMAT))
    Environment.setProperty("rcugas.sqoop.date", convertDateTimeInStringWithFormat(rcugasSqoopDate, PROPERTIES_YEAR_MOTNHS_DAYS_FORMAT))
    Environment.setProperty("session", session)
  }
}
