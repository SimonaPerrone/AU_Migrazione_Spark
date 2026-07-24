package it.eng.au.calcoloIndennizzi.schema

import it.eng.au.indennizziMisureGasCommon.schema.SchemaEnum
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ExclusionFilterSchema extends SchemaEnum {
  val pdr, file = Value

  val schema: StructType = StructType(Array(
    StructField(pdr, StringType, nullable = true),
    StructField(file, StringType, nullable = true)
  ))
}