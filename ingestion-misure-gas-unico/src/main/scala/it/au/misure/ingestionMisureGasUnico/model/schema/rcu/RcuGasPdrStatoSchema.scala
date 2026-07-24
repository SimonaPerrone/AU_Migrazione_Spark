package it.au.misure.ingestionMisureGasUnico.model.schema.rcu

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum

object RcuGasPdrStatoSchema extends SchemaEnum {
  val n_id_pdr_stato , 
  n_id_pdr ,
  t_cod_stato_pdr ,
  d_data_inizio ,
  d_data_fine ,
  t_cod_causale_no_disat ,
  d_aggiornamento ,
  n_id_traccia ,
  n_id_s_prec ,
  b_processo_in_corso ,
  t_cod_causale_no_riatt ,
  tipo_data_inizio ,
  tipo_data_fine ,
  t_note ,
  d_data_rif = Value
}
