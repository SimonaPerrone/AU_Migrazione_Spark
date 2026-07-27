package it.eng.au.ammissibilitaSettlementGas.model.rules

import it.eng.au.ammissibilitaSettlementGas.model.{ReportMessage, Rule, TFCMetadata, VPGMetadata}

case class TFCCsvRule  (
                         condition: TFCMetadata => Boolean,
                         override val message: ReportMessage,
                         override val ruleName: String = "",
                         override val isEnabled: Boolean = true
                       ) extends Rule
