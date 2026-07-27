package it.eng.au.ammissibilitaSettlementGas.model

case class VPG(
                fileName: String,
                fields: List[String],
                numeroRiga:Int,
                giornoRiferimento: Option[String] = None,
                C1A1: Option[String] = None,
                C1B1: Option[String] = None,
                C1C1: Option[String] = None,
                C1D1: Option[String] = None,
                C1E1: Option[String] = None,
                C1F1: Option[String] = None,
                C1A2: Option[String] = None,
                C1B2: Option[String] = None,
                C1C2: Option[String] = None,
                C1D2: Option[String] = None,
                C1E2: Option[String] = None,
                C1F2: Option[String] = None,
                C1A3: Option[String] = None,
                C1B3: Option[String] = None,
                C1C3: Option[String] = None,
                C1D3: Option[String] = None,
                C1E3: Option[String] = None,
                C1F3: Option[String] = None,
                C2: Option[String] = None,
                C4: Option[String] = None,
                T11: Option[String] = None,
                T12: Option[String] = None,
                T13: Option[String] = None,
                isAmmissibile: Boolean = true, // We initialize it true.
                statusCode: String = "",
                statusMessage: String = ""
              )

