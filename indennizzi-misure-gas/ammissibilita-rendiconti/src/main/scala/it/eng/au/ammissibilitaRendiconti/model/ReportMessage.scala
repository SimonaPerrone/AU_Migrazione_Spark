package it.eng.au.ammissibilitaRendiconti.model

case class ReportMessage(
                          isAmmissibile: Boolean = true,
                          statusCode: String = "",
                          statusMessage: String = ""
                        ) extends Serializable
