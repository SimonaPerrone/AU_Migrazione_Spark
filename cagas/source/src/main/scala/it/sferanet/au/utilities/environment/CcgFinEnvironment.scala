package it.sferanet.au.utilities.environment

import it.sferanet.au.utilities.DataframeUtils.getMassivoExecutionId
import it.sferanet.au.utilities.args.FlowArgsConfig
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.log4j.Logger
import org.joda.time.{DateTime, LocalDate}

object CcgFinEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"${Constants.CCG_CA_LOG} Start CCG CAGas FIN")

    Environment.getOrCreate("[CCG] Calcolo Consumo Annuo Gas FIN", flowArgsConfig.pathToProperties)

    logger.warn(s"${Constants.CCG_CA_LOG} Run CCG CAGas FIN")

    val sysDate = flowArgsConfig.dateToRun.getOrElse(new DateTime(Environment.executionId))
    set(sysDate)

    logger.warn(s"${Constants.CCG_CA_LOG} Properties:")
    logger.warn(s"${Constants.CCG_CA_LOG} ${Environment.printProperties}")
    logger.warn(s"${Constants.CCG_CA_LOG} Execution ID: ${Environment.executionId}")
    logger.warn(s"${Constants.CCG_CA_LOG} Date: ${Environment.getPartitionDate}")
    logger.warn(s"${Constants.CCG_CA_LOG} applicationID=${Environment.getSpark.sparkContext.applicationId}")
  }

  def set(sysDate: DateTime): Unit = {
    val annoTermicoSysDate = if (sysDate.isBefore(sysDate.withMonthOfYear(10).withDayOfMonth(1))) sysDate.plusYears(1).getYear else sysDate.plusYears(2).getYear

    val flowReadStartDate = new LocalDate().withMonthOfYear(10).withYear(annoTermicoSysDate).minus(Environment.getFlowStartDateTimeBack)
    val flowReadEndDate = new LocalDate().withMonthOfYear(5).withYear(annoTermicoSysDate).minus(Environment.getFlowEndDateTimeBack)
    val flowReadReceiveEndDate = sysDate.minus(Environment.getFlowReadReceiveEndDateTimeBack)
    val tdsReadReceiveEndDate = sysDate.minus(Environment.getTdsReadReceiveEndDateTimeBack)
    val contractContinuityUpperBoundDate = new LocalDate().withDayOfMonth(1).withMonthOfYear(6).withYear(annoTermicoSysDate).minus(Environment.getContractContinuityUpperBoundDateTimeBack)
    val sessione = "CCG"
    val tipoTrasmissione = "AGG_FIN"
    val filterMode = "noFilter"
    val pdrMassivoAnnoCompetenza = annoTermicoSysDate
    val zSupDate = flowReadEndDate.withDayOfMonth(31)
    val zInfDate = zSupDate.minus(Environment.getZInfDateTimeBackWrtZSupDate)

    val rcugasMassivoExecutionId = getMassivoExecutionId()

    Environment.setProperty("flow.read.startDate", flowReadStartDate.toString("yyyyMM"))
    Environment.setProperty("flow.read.endDate", flowReadEndDate.toString("yyyyMM"))
    Environment.setProperty("flow.read.receive.endDate", flowReadReceiveEndDate.toString("yyyyMMdd"))
    Environment.setProperty("tds.read.receive.endDate", tdsReadReceiveEndDate.toString("yyyy-MM-dd"))
    Environment.setProperty("contractContinuity.upperBound.data", contractContinuityUpperBoundDate.toString("yyyy-MM-dd"))
    Environment.setProperty("sessione", sessione)
    Environment.setProperty("tipoTrasmissione.value", tipoTrasmissione)
    Environment.setProperty("filterPdr.mode", filterMode)
    Environment.setProperty("pdr_massivo.anno_competenza", pdrMassivoAnnoCompetenza.toString)
    Environment.setProperty("rcugas.massivo.execution_id", rcugasMassivoExecutionId)
    Environment.setProperty("z.sup.date", zSupDate.toString("yyyy-MM-dd"))
    Environment.setProperty("z.inf.date", zInfDate.toString("yyyy-MM-dd"))
  }

}
