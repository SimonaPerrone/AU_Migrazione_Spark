package it.eng.au.pubblicazioneIndennizzi.utility

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.args.PubblicazioneArgsConfig
import it.eng.au.pubblicazioneIndennizzi.utility.Constants.{APPLICATION_NAME, LOG_NAME}

object PubblicazioneIndennizziEnvironment {
  def setEnvironment(parsedArgs: PubblicazioneArgsConfig): Unit = {
    val applicationName = APPLICATION_NAME
    val logName = LOG_NAME

    Environment.getOrCreate(applicationName, logName, parsedArgs.propertiesPath)

    Environment.setProperty("recovery.mode", parsedArgs.recoveryMode.toString)
    if (Properties.isRecoveryMode)
      Environment.setProperty("input.table.executionid", parsedArgs.inputTableExecutionId)
  }
}