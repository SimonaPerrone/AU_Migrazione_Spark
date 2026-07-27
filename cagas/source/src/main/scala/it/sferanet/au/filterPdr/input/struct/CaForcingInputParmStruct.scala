package it.sferanet.au.filterPdr.input.struct

import it.sferanet.au.filterPdr.input.schema.CaForcingInputParmSchema
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object CaForcingInputParmStruct {
  lazy val struct = StructType(Array(
    StructField(CaForcingInputParmSchema.pdr.toString, StringType),
    StructField(CaForcingInputParmSchema.ca.toString, DoubleType),
    StructField(CaForcingInputParmSchema.codPrel.toString, StringType),
    StructField(CaForcingInputParmSchema.catUso.toString, StringType),
    StructField(CaForcingInputParmSchema.zonClimatica.toString, StringType),
    StructField(CaForcingInputParmSchema.classePrelievo.toString, StringType),
    StructField(CaForcingInputParmSchema.trattamento.toString, StringType)
  ))
}