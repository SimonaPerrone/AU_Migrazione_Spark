package it.au.misure.ingestionMisureGasUnico.model.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CsvRecoverySchema extends SchemaEnum {
  val filename_src = Value

  val schema: StructType = StructType(Array(
    StructField(filename_src, StringType, nullable = true)
  ))
}