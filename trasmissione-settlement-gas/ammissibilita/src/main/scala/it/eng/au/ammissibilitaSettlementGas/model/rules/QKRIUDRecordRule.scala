package it.eng.au.ammissibilitaSettlementGas.model.rules

import it.eng.au.ammissibilitaSettlementGas.model.{QKRIUD, ReportMessage, Rule}

case class QKRIUDRecordRule(
                             condition: QKRIUD => Boolean,
                             override val message: ReportMessage,
                             override val ruleName: String = "",
                             override val isEnabled: Boolean = true
                           ) extends Rule