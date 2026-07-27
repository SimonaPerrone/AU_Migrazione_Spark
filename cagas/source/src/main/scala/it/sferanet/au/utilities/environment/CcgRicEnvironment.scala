package it.sferanet.au.utilities.environment

import it.sferanet.au.schema.CaPreFinalSchema
import it.sferanet.au.utilities.DataframeUtils.getMassivoExecutionId
import it.sferanet.au.utilities.args.FlowArgsConfig
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.log4j.Logger
import org.joda.time.{DateTime, LocalDate}

object CcgRicEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"${Constants.CCG_CA_LOG} Start CCG CAGas RIC")

    Environment.getOrCreate("[CCG] Calcolo Consumo Annuo Gas RIC", flowArgsConfig.pathToProperties)

    logger.warn(s"${Constants.CCG_CA_LOG} Run CCG CAGas RIC")

    val sysDate = flowArgsConfig.dateToRun.getOrElse(new DateTime(Environment.executionId))
    set(sysDate)

    logger.warn(s"${Constants.CCG_CA_LOG} Properties:")
    logger.warn(s"${Constants.CCG_CA_LOG} ${Environment.printProperties}")
    logger.warn(s"${Constants.CCG_CA_LOG} Execution ID: ${Environment.executionId}")
    logger.warn(s"${Constants.CCG_CA_LOG} Date: ${Environment.getPartitionDate}")
    logger.warn(s"${Constants.CCG_CA_LOG} applicationID=${Environment.getSpark.sparkContext.applicationId}")
  }

  def set(sysDate: DateTime): Unit = {
    val annoTermicoSysDate = if (sysDate.isBefore(sysDate.withMonthOfYear(10).withDayOfMonth(1)))
      sysDate.getYear
    else
      sysDate.plusYears(1).getYear

    val flowReadStartDate = new LocalDate().withMonthOfYear(10).withYear(annoTermicoSysDate).minus(Environment.getFlowStartDateTimeBack)
    val flowReadEndDate = new LocalDate().withMonthOfYear(5).withYear(annoTermicoSysDate).minus(Environment.getFlowEndDateTimeBack)
    val flowReadReceiveEndDate = sysDate.minus(Environment.getFlowReadReceiveEndDateTimeBack)
    val tdsReadReceiveEndDate = sysDate.minus(Environment.getTdsReadReceiveEndDateTimeBack)
    val contractContinuityUpperBoundDate = new LocalDate().withDayOfMonth(1).withMonthOfYear(6).withYear(annoTermicoSysDate).minus(Environment.getContractContinuityUpperBoundDateTimeBack)
    val sessione = "CCG"
    val tipoTrasmissione = "AGG_RIC"
    val filterMode = "perimetroAggRic"
    val pdrMassivoAnnoCompetenza = annoTermicoSysDate.toString
    val zSupDate = flowReadEndDate.withDayOfMonth(31)
    val zInfDate = zSupDate.minus(Environment.getZInfDateTimeBackWrtZSupDate)

    val caPreFinalExecutionId = getCaPreFinalExecutionId(annoTermicoSysDate)
    val rcugasMassivoExecutionId = getMassivoExecutionId()

    Environment.setProperty("flow.read.startDate", flowReadStartDate.toString("yyyyMM"))
    Environment.setProperty("flow.read.endDate", flowReadEndDate.toString("yyyyMM"))
    Environment.setProperty("flow.read.receive.endDate", flowReadReceiveEndDate.toString("yyyyMMdd"))
    Environment.setProperty("tds.read.receive.endDate", tdsReadReceiveEndDate.toString("yyyy-MM-dd"))
    Environment.setProperty("contractContinuity.upperBound.data", contractContinuityUpperBoundDate.toString("yyyy-MM-dd"))
    Environment.setProperty("sessione", sessione)
    Environment.setProperty("tipoTrasmissione.value", tipoTrasmissione)
    Environment.setProperty("filterPdr.mode", filterMode)
    Environment.setProperty("pdr_massivo.anno_competenza", pdrMassivoAnnoCompetenza)
    Environment.setProperty("rcugas.massivo.execution_id", rcugasMassivoExecutionId)
    Environment.setProperty("ca_pre_final.execution_id", caPreFinalExecutionId)
    Environment.setProperty("z.sup.date", zSupDate.toString("yyyy-MM-dd"))
    Environment.setProperty("z.inf.date", zInfDate.toString("yyyy-MM-dd"))
  }

  def getCaPreFinalExecutionId(annoTermico: Int): String = {
    val listPartitions = Environment.getSpark.sqlContext
      .sql(s"SHOW PARTITIONS ${Environment.getCaPreFinalTableName}")
      .collect
      .toList
      .map(_.getString(0))

    val valuePartition = listPartitions
      .filter(value =>
        value.contains(s"${CaPreFinalSchema.session.toString}=CDP")
        && value.contains(s"${CaPreFinalSchema.tipo_trasmissione.toString}=AGG_FIN")
        && value.contains(s"${CaPreFinalSchema.anno_competenza}=${annoTermico.toString}")
      )
      .map(
        _.split("/")
          .filter(_.contains(CaPreFinalSchema.executionid.toString))
          .head
          .split("=").last
      )
      .sorted
      .reverse
      .head

    valuePartition
  }

}
