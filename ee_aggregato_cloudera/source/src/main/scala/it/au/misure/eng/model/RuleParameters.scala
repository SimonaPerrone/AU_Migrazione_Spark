package it.au.misure.eng.model

case class RuleParameters(
                           isActive: Boolean,
                           bloccante: Boolean,
                           parameters: Map[String, String]
                         ) extends Serializable {
  override def toString: String = s"{isActive: $isActive, bloccante: $bloccante, parameters: $parameters}"
}