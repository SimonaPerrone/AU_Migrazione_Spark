package it.eng.au.ammissibilitaSettlementGas.utility.environment

import it.eng.au.ammissibilitaSettlementGas.utility.Constants.{APPLICATION_NAME, LOG_NAME}
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import it.eng.au.ammissibilitaSettlementGas.args.Args
import it.eng.au.ammissibilitaSettlementGas.args.ArgsFactory.logger

import java.time.{LocalDate, YearMonth}
import java.time.format.DateTimeFormatter

object AmmissibilitaEnvironment {
  def setEnvironment(parsedArgs:Args):Unit = {
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

    val today = LocalDate.now()
    val yesterday = today.minusDays(1)

    val year = if (parsedArgs.isRecoveryMode) parsedArgs.annoMese.get.substring(0, 4) // get year (yyyy) from "yyyyMM".
      else yesterday.format(DateTimeFormatter.ofPattern("yyyy"))
    val month = if (parsedArgs.isRecoveryMode) parsedArgs.annoMese.get.substring(4, 6)  // get month (MM) from "yyyyMM".
    else yesterday.format(DateTimeFormatter.ofPattern("MM"))

    Environment.setProperty("current.year", year)
    Environment.setProperty("current.month", month)
  }

  /*
  def setEnvironment(parsedArgs: RendicontiArgs): Unit = {
    val applicationName = APPLICATION_NAME
    val logName = CIG_LOG

    Environment.getOrCreate(applicationName, logName, parsedArgs.propertiesPath)

    Environment.setProperty("recovery.mode", parsedArgs.recoveryMode.toString)

    val today = LocalDate.now()
    val yesterday = today.minusDays(1)

    // If it's not a recovery run, then we get the files from year/month directory, where year and month are computed from the day before the run
    if (!Properties.isRecoveryMode) {
      Environment.setProperty("current.year", yesterday.format(DateTimeFormatter.ofPattern("yyyy")))
      Environment.setProperty("current.month", yesterday.format(DateTimeFormatter.ofPattern("MM")))
    }
    else {
      Environment.setProperty("current.year", today.format(DateTimeFormatter.ofPattern("yyyy")))
      Environment.setProperty("current.month", today.format(DateTimeFormatter.ofPattern("MM")))
    }
  }

   */
}
