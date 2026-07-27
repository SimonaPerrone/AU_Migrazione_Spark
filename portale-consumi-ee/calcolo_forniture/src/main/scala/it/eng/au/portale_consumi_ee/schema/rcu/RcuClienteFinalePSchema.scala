package it.eng.au.portale_consumi_ee.schema.rcu

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object RcuClienteFinalePSchema extends SchemaEnum{
  val
                  n_id_cliente,
                  t_nome,
                  t_cognome,
                  t_ragsoc,
                  b_persona_fisica,
                  t_cf,
                  t_piva,
                  n_id_sedelegale,
                  t_email,
                  t_codice_ateco,
                  b_diritto_mt,
                  d_autocert_mt,
                  t_nota,
                  d_aggiornamento,
                  n_id_traccia,
                  n_id_s_prec,
                  t_denom,
                  t_dettaglio_cf,
                  t_dettaglio_piva
  = Value
}
