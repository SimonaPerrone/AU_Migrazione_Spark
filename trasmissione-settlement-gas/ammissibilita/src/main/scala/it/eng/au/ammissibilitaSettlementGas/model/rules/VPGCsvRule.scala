package it.eng.au.ammissibilitaSettlementGas.model.rules

import it.eng.au.ammissibilitaSettlementGas.model.{ReportMessage, Rule, VPGMetadata}

case class VPGCsvRule  (
                      condition: VPGMetadata => Boolean,
                      override val message: ReportMessage,
                      override val ruleName: String = "",
                      override val isEnabled: Boolean = true
  ) extends Rule

