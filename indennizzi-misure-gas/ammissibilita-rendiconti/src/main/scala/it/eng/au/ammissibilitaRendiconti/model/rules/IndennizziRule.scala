package it.eng.au.ammissibilitaRendiconti.model.rules

import it.eng.au.ammissibilitaRendiconti.model.{AggregatoTotale, ReportMessage, ZipRzg1Metadata}

case class IndennizziRule(
                           condition: (ZipRzg1Metadata, Option[AggregatoTotale]) => Boolean,
                           override val message: ReportMessage,
                           override val ruleName: String = "",
                           override val isEnabled: Boolean = true
                         ) extends Rule
