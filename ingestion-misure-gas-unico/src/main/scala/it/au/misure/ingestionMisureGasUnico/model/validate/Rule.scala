package it.au.misure.ingestionMisureGasUnico.model.validate

import it.au.misure.ingestionMisureGasUnico.model.GasMetadata

import scala.xml.NodeSeq

case class Rule(
                 condition: (NodeSeq, GasMetadata, Option[RuleParameters]) => Boolean,
                 message: ReportMessage,
                 ruleName: String = "",
                 isActive: Boolean = true,
                 parameter: Option[RuleParameters] = None
               ) extends Serializable {
  override def toString: String = {
    s"{rulename: $ruleName, isActive: $isActive, message: $message, parameter = $parameter}"
  }
}
