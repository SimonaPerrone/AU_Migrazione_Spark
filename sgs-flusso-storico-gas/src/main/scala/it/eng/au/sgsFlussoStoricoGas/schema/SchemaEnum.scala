package it.eng.au.sgsFlussoStoricoGas.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

trait SchemaEnum extends Enumeration {

  def getValues: List[String] = this.values.toList.map(_.toString)

  implicit def valueToString(value: Value) : String = value.toString

  def createSparkSchema(values:List[String] = getValues): StructType = {
    val structList = values.map(x => StructField(x, StringType))
    StructType(structList)
  }
}

