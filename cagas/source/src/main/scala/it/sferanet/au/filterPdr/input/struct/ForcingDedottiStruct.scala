package it.sferanet.au.filterPdr.input.struct

import it.sferanet.au.filterPdr.input.schema.ForcingDedottiSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ForcingDedottiStruct {
  lazy val struct = StructType(Array(
    StructField(ForcingDedottiSchema.pdr.toString, StringType)
  ))
}
