package it.eng.au.queryReport.schema

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum

object ElencoFlussiDIQuerySchema extends SchemaEnum {
  val pdr,
  piva_distr,
  piva_it,
  piva_udd,
  piva_udb,
  piva_rdb,
  service,
  read_type,
  motivation,
  serial_number_mis,
  serial_number_conv,
  measure,
  converted,
  cau_int_mis,
  cau_int_corr,
  trattamento,
  sessione,
  nome_file,
  dailyconsumption_executionid,
  annomese,
  data_lettura,
  coeff_cor,
  executionid
  = Value
}
