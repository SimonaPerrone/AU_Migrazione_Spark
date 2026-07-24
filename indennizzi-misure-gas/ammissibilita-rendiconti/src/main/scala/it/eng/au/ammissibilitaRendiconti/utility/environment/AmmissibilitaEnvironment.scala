package it.eng.au.ammissibilitaRendiconti.utility.environment

import it.eng.au.ammissibilitaRendiconti.args.RendicontiArgs
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants.{APPLICATION_NAME, CIG_LOG}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object AmmissibilitaEnvironment {
  def setEnvironment(parsedArgs: RendicontiArgs): Unit = {
    val applicationName = APPLICATION_NAME
    val logName = CIG_LOG

    Environment.getOrCreate(applicationName, logName, parsedArgs.propertiesPath)
    Environment.setProperty("recovery.mode", parsedArgs.recoveryMode.toString)

    // If we passed an executionid as input, we override the default executionid with it
    if (parsedArgs.outputExecutionid.nonEmpty)
      Environment.executionId = parsedArgs.outputExecutionid.get.toLong

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
}
