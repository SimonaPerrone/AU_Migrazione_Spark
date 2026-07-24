package it.eng.au.ammissibilitaRendiconti.model.rules

import it.eng.au.ammissibilitaRendiconti.model.{CsvRzg1Metadata, ReportMessage}

case class CsvRule(
                    condition: CsvRzg1Metadata => Boolean,
                    override val message: ReportMessage,
                    override val ruleName: String = "",
                    override val isEnabled: Boolean = true
                  ) extends Rule
