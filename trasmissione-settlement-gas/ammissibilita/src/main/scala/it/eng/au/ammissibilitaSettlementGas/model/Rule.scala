package it.eng.au.ammissibilitaSettlementGas.model

trait Rule extends Serializable {
  val message: ReportMessage
  val ruleName: String = ""
  val isEnabled: Boolean = true
}
