package it.eng.au.portale_consumi_ee.schema.rcus

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object RcusPodstatoPSchema extends SchemaEnum {
  val
  n_id_scheda,
  n_id_pod,
  t_stato_attivazione,
  d_attivazione,
  d_disattivazione,
  t_causale_no_riattiv,
  t_causale_no_disattiv,
  t_stato_sosp,
  d_sospensione,
  d_revoca_sosp,
  t_causale_no_sosp,
  t_switching,
  t_nota,
  d_aggiornamento,
  d_archiviazione,
  n_id_traccia,
  n_id_s_prec,
  n_id_s_succ,
  b_valido,
  t_cod_disattivazione
  = Value
}
