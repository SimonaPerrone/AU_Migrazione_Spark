package it.eng.au.ammissibilitaSettlementGas.model.rules

import it.eng.au.ammissibilitaSettlementGas.model.{QKRIUDMetadata, ReportMessage, Rule}

case class QKRIUDCsvRule(
                          condition: QKRIUDMetadata => Boolean,
                          override val message: ReportMessage,
                          override val ruleName: String = "",
                          override val isEnabled: Boolean = true
                        ) extends Rule
