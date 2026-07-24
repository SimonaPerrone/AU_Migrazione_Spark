package it.eng.au.pubblicazione_cce.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.language.implicitConversions

trait SchemaEnum extends Enumeration {
  def getValues: List[String] = this.values.toList.map(_.toString)

  implicit def valueToString(value: Value): String = value.toString

}
