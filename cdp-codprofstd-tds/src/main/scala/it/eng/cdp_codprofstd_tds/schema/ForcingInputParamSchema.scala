package it.eng.cdp_codprofstd_tds.schema

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object ForcingInputParamSchema extends SchemaEnum {
  val pdr,
  ca,
  codPrel,
  catUso,
  zonClimatica,
  classePrelievo,
  trattamento = Value

  val schema: StructType =
    StructType(
      StructField(pdr, StringType) ::
      StructField(ca, DoubleType) ::
      StructField(codPrel, StringType) ::
      StructField(catUso, StringType) ::
      StructField(zonClimatica, StringType) ::
      StructField(classePrelievo, StringType) ::
      StructField(trattamento, StringType) ::
      Nil)
}
