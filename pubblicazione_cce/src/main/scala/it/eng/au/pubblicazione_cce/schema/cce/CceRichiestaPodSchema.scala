package it.eng.au.pubblicazione_cce.schema.cce

import it.eng.au.pubblicazione_cce.schema.SchemaEnum

object CceRichiestaPodSchema extends SchemaEnum {
  val
  n_id_richiesta,
  t_servizio,
  t_processo,
  d_data_richiesta,
  t_anno,
  t_mese,
  t_ruolo,
  t_piva,
  t_codice_pod,
  b_ammissibilita,
  t_cod_causale,
  t_motivazione,
  t_nome_file,
  t_tipo_amm,
  sqoop_date,
  partition_request_date
  = Value
}
