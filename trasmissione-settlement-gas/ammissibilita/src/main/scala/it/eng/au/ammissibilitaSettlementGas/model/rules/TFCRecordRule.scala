package it.eng.au.ammissibilitaSettlementGas.model.rules

import it.eng.au.ammissibilitaSettlementGas.model.{ReportMessage, Rule, TFC, TFCMetadata}

case class TFCRecordRule(
                          condition: TFC => Boolean,
                          override val message: ReportMessage,
                          override val ruleName: String = "",
                          override val isEnabled: Boolean = true
                        ) extends Rule
