package it.eng.au.pubblicazioneIndennizzi.args

case class PubblicazioneArgsConfig(
                                    propertiesPath: String = null,
                                    recoveryMode: Boolean = false,
                                    inputTableExecutionId: String = null
                                  )
