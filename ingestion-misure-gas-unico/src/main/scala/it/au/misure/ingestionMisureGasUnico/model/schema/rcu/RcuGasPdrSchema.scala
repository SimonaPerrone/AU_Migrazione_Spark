package it.au.misure.ingestionMisureGasUnico.model.schema.rcu

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum

object RcuGasPdrSchema extends SchemaEnum {
  val n_id_pdr
  , t_codice_pdr
  , t_cod_tipo_pdr
  , t_codice_istat
  , t_note
  , d_aggiornamento
  , n_id_traccia
  , n_id_s_prec
  , n_id_indirizzo
  , d_data_rif
  , t_disalimentabilita
  , t_accesso_ui = Value
}
