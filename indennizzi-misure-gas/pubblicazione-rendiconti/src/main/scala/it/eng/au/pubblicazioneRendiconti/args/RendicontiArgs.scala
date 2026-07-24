package it.eng.au.pubblicazioneRendiconti.args

case class RendicontiArgs(
                           propertiesPath: String = null,
                           recoveryMode: Boolean = false,
                           inputTableExecutionId: String = null
                         )