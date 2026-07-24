package it.au.misure.ingestionMisureGasUnico.model.schema.standard

object TracciatoStandardEnum extends Enumeration {
  type TracciatoStandardEnum = Value
  val M, R = Value

  implicit def valueToString(value: Value): String = value.toString

  def getValues: List[String] = this.values.toList.map(_.toString)
}
