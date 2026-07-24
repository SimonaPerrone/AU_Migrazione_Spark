package it.eng.au.calcoloIndennizzi.schema

import it.eng.au.indennizziMisureGasCommon.schema.SchemaEnum
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RecoveryCsvSchema extends SchemaEnum {
  val piva_distr,
  piva_udd
  = Value

  val schema: StructType = StructType(Array(
    StructField(piva_distr, StringType, nullable = true),
    StructField(piva_udd, StringType, nullable = true)
  ))
}
