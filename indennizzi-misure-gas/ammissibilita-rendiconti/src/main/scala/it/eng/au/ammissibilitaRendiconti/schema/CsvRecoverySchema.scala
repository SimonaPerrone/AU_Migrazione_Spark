package it.eng.au.ammissibilitaRendiconti.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CsvRecoverySchema extends SchemaEnum {
  val file = Value

  val schema: StructType = StructType(Array(
    StructField(file, StringType, nullable = true)
  ))
}