package it.au.misure.ingestionMisureGasUnico.model.validate

case class RuleParameters(
                           isActive: Boolean,
                           bloccante: Boolean,
                           parameters: Map[String, String]
                         ) extends Serializable {
  override def toString: String = s"{isActive: $isActive, bloccante: $bloccante, parameters: $parameters}"
}
