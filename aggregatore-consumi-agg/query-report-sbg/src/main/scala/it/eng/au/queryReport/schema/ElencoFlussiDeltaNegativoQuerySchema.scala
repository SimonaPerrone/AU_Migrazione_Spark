package it.eng.au.queryReport.schema

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum

object ElencoFlussiDeltaNegativoQuerySchema extends SchemaEnum {
  val
  pdr,
  nome_file,
  sessione,
  annomese,
  measure,
  converted,
  data_lettura,
  read_type,
  serial_number_mis,
  serial_number_conv,
  coeff_cor,
  piva_distr,
  piva_udb,
  piva_udd,
  dailyconsumption_executionid,
  executionid
  = Value
}
