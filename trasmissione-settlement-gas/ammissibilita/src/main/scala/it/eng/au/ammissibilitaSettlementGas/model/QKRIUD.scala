package it.eng.au.ammissibilitaSettlementGas.model

case class QKRIUD(
                   fileName: String,
                   fields: List[String],
                   numeroRiga:Int,
                   data: Option[String] = None,
                   codRemi: Option[String] = None,
                   qkriud: Option[String] = None,
                   isAmmissibile: Boolean = true, // We initialize it true.
                   statusCode: String = "",
                   statusMessage: String = ""
                 )
