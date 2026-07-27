package it.eng.au.queryReport.schema

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum

object SospesiQuerySchema extends SchemaEnum {
  val
  pdr,
  piva_distr,
  piva_it,
  piva_udd,
  piva_udb,
  piva_rdb,
  data_inizio_sosp,
  data_revoca_sosp,
  motivazione_sosp,
  dtg,
  cod_remi,
  id_reg_clim,
  cod_prof_prel_std,
  trattamento,
  tipo_cliente,
  sessione,
  dailyconsumption_executionid,
  annomese,
  executionid
  = Value
}
