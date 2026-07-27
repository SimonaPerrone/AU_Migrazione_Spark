package it.sferanet.au.filterPdr.input.struct

import it.sferanet.au.filterPdr.input.schema.ExclusioneFilePdrPdrSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ExclusioneFilePdrPdrStruct {
  lazy val struct = StructType(Array(
    StructField(ExclusioneFilePdrPdrSchema.pdr.toString, StringType)
  ))
}
