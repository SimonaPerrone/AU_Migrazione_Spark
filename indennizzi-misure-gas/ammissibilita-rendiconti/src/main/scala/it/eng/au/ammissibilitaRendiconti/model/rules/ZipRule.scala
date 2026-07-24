package it.eng.au.ammissibilitaRendiconti.model.rules

import it.eng.au.ammissibilitaRendiconti.model.{ReportMessage, ZipRzg1Metadata}

case class ZipRule(
                    condition: ZipRzg1Metadata => Boolean,
                    override val message: ReportMessage,
                    override val ruleName: String = "",
                    override val isEnabled: Boolean = true
                  ) extends Rule
