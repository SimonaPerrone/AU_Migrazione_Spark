package it.au.misure.calcolo_capacita.component.schema

trait Schema extends Enumeration {

  implicit def valueToString(value: Value): String = value.toString

  def getValues: List[String] = this.values.toList.map(_.toString)


}
