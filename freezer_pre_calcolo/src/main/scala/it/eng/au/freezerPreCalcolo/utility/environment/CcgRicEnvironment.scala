package it.eng.au.freezerPreCalcolo.utility.environment

import it.eng.au.freezerPreCalcolo.utility.Constants
import it.eng.au.freezerPreCalcolo.utility.args.FlowArgsConfig
import org.apache.log4j.Logger
import org.joda.time.{DateTime, LocalDate}

object CcgRicEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"${Constants.CCG_LOG} Start Freezer procedure")

    Environment.getOrCreate("[CCG] Freezer", flowArgsConfig.pathToProperties)

    val sysDate = flowArgsConfig.dateToRun.getOrElse(new DateTime(Environment.executionId))
    set(sysDate)

    if (flowArgsConfig.dateToRun.isDefined) logger.warn(s"${Constants.CCG_LOG} The date ${flowArgsConfig.dateToRun.get} passed as argument is ignored")
    logger.warn(s"${Constants.CCG_LOG} Run Freezer")
    logger.warn(s"${Constants.CCG_LOG} Properties:")
    logger.warn(s"${Constants.CCG_LOG} ${Environment.printProperties}")
    logger.warn(s"${Constants.CCG_LOG} Execution ID: ${Environment.executionId}")
    logger.warn(s"${Constants.CCG_LOG} Date: ${Environment.getPartitionDate}")
    logger.warn(s"${Constants.CCG_LOG} applicationID=${Environment.getSpark.sparkContext.applicationId}")
  }

  def set(sysDate: DateTime): Unit = {
    val annoTermicoSysDate = if (sysDate.isBefore(sysDate.withMonthOfYear(10).withDayOfMonth(1))) sysDate.getYear else sysDate.plusYears(1).getYear
    val freezeDate = new LocalDate().withDayOfMonth(1).withMonthOfYear(6).withYear(annoTermicoSysDate).minusYears(1)
    val session = "CCG"

    Environment.setProperty("freeze_date", freezeDate.toString("yyyy-MM-dd"))
    Environment.setProperty("session", session)
  }
}
