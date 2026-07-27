package it.sferanet.au.schema

import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

object SettleGasGasTdsSchema extends SchemaEnum {
  val valid,
  file_name,
  file_name_rel,
  cod_pdr,
  cat_uso,
  classe_prelievo,
  tipol_uso,
  cod_prof_prel_std,
  piva_utente,
  cod_causale,
  descrizione,
  num_riga,
  data_creazione,
  data_elab
  = Value

  override def createSparkSchema(values: List[String] = getValues): StructType = {
    StructType(
      super.createSparkSchema(values)
        .map(sf => if (sf.name.equals(valid.toString)) StructField(valid.toString, BooleanType) else sf)
    )
  }
}
