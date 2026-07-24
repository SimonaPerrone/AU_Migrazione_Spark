package it.eng.au.ammissibilitaRendiconti.args

case class RendicontiArgs(
                           propertiesPath: String = null,
                           recoveryMode: Boolean = false,
                           outputExecutionid: Option[String] = None
                         )