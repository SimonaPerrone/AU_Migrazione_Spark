package it.eng.au.pubblicazione_cce.schema.cce

import it.eng.au.pubblicazione_cce.schema.SchemaEnum

object CceRichiestaFiltroSchema extends SchemaEnum {
  val
  n_id_richiesta,
  t_tipo,
  t_servizio,
  t_processo,
  d_data_richiesta,
  t_anno,
  t_mese,
  t_ruolo,
  t_piva,
  t_tensione,
  t_zona,
  t_tipo_pod,
  t_piva_udd,
  t_piva_id,
  t_codice_terna,
  t_tariffa,
  sqoop_date,
  partition_request_date
  = Value
}
