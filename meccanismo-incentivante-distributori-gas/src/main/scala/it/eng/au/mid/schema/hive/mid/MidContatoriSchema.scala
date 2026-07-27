package it.eng.au.mid.schema.hive.mid

import it.eng.au.mid.schema.SchemaEnum

object MidContatoriSchema extends SchemaEnum {
  val
  pdr,
  contatore,
  stato,
  treatment,
  data_tracciatura,
  processo_tracciatura,
  sessione_tracciatura,
  causale_tracciatura,
  tipo_calcolo,
  executionid_daily_consumption,
  executionid_tracciatura_prev,
  annomese,
  executionid_tracciatura
  = Value
}
