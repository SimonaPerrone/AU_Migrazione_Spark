package it.eng.au.ammissibilitaRendiconti.model.rules

import it.eng.au.ammissibilitaRendiconti.model.ReportMessage

trait Rule extends Serializable {
  val message: ReportMessage
  val ruleName: String = ""
  val isEnabled: Boolean = true
}
