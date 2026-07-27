package it.eng.au.calcoloSettlementGas.utility.environment

import it.eng.au.calcoloSettlementGas.args.Args
import it.eng.au.calcoloSettlementGas.utility.Constants.{APPLICATION_NAME, LOG_NAME}
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.log4j.Logger

import java.time.YearMonth
import java.time.format.DateTimeFormatter

object CalcoloEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def setEnvironment(parsedArgs: Args): Unit = {
    val applicationName = APPLICATION_NAME
    val logName = LOG_NAME

    Environment.getOrCreate(applicationName, logName, parsedArgs.propertiesPath)

    if (parsedArgs.isRecoveryMode)
      try {
        YearMonth.parse(parsedArgs.annoMese.get, DateTimeFormatter.ofPattern("yyyyMM"))
      } catch {
        case e: Exception =>
          logger.error(s"Year-month parameter passed as input ${parsedArgs.annoMese.get} has an invalid format; format required -m yyyyMM.")
          throw new IllegalArgumentException(s"Year-month parameter passed as input ${parsedArgs.annoMese.get} has an invalid format; format required -m yyyyMM.")
      }

    val annoMese = if (parsedArgs.isRecoveryMode) YearMonth.parse(parsedArgs.annoMese.get, DateTimeFormatter.ofPattern("yyyyMM"))
    else YearMonth.now().minusMonths(Properties.getMonthDifferenceTimeBack.toInt)

    val year = annoMese.getYear
    val month = annoMese.getMonth
    val yearMonthObject = YearMonth.of(year, month)
    val daysInMonth = yearMonthObject.lengthOfMonth().toString

    Environment.setProperty("annomese", annoMese.format(DateTimeFormatter.ofPattern("yyyyMM")))
    Environment.setProperty("days.in.month", daysInMonth)
  }
}
