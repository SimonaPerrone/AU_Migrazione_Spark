package it.sferanet.au.filterPdr.input.struct

import it.sferanet.au.filterPdr.input.schema.ExclusioneFilePdrFileSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ExclusioneFilePdrFileStruct {
  lazy val struct = StructType(Array(
    StructField(ExclusioneFilePdrFileSchema.localFile.toString, StringType)
  ))
}
