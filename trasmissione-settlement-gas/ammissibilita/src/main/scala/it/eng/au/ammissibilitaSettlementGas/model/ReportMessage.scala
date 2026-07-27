package it.eng.au.ammissibilitaSettlementGas.model

case class ReportMessage(
                          isAmmissibile: Boolean = true,
                          statusCode: String = "",
                          statusMessage: String = ""
                        ) extends Serializable
