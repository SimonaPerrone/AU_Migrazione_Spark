package it.eng.au.pubblicazioneRendiconti.utility

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneRendiconti.args.RendicontiArgs
import it.eng.au.pubblicazioneRendiconti.utility.constants.Constants.{APPLICATION_NAME, CIG_LOG}
import it.eng.au.pubblicazioneRendiconti.utility.properties.Properties

object PubblicazioneRendicontiEnvironment {
  def setEnvironment(parsedArgs: RendicontiArgs): Unit = {
    val applicationName = APPLICATION_NAME
    val logName = CIG_LOG

    Environment.getOrCreate(applicationName, logName, parsedArgs.propertiesPath)

    Environment.setProperty("recovery.mode", parsedArgs.recoveryMode.toString)
    if (Properties.isRecoveryMode)
      Environment.setProperty("cig.indennizziRzg2.executionId", parsedArgs.inputTableExecutionId)
  }
}
