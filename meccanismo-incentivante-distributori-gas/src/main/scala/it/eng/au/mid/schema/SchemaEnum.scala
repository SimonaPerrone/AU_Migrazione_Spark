package it.eng.au.mid.schema

import scala.language.implicitConversions

trait SchemaEnum extends Enumeration {

  def getValues: List[String] = this.values.toList.map(_.toString)

  implicit def valueToString(value: Value) : String = value.toString

}
