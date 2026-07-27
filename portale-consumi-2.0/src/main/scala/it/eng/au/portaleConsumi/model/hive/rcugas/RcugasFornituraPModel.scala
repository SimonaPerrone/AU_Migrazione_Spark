package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasFornituraPModel(
                                  n_id_fornitura: String = null,
                                  d_data_inizio: Timestamp = null,
                                  d_data_fine: Timestamp = null,
                                  n_id_cliente: String = null,
                                  n_id_pdr: String = null,
                                  n_id_vend: String = null,
                                  b_tariffa_tm: String = null,
                                  t_codice_ateco: String = null,
                                  n_lettura_attivazione: String = null,
                                  t_aliquota_iva: String = null,
                                  t_imposte: String = null,
                                  n_indirizzo_fornitura: String = null,
                                  n_indirizzo_recap: String = null,
                                  t_bonus_gas: String = null,
                                  d_data_inizio_bonus: Timestamp = null,
                                  d_data_fine_bonus: Timestamp = null,
                                  b_prestazioni_non_concluse: String = null,
                                  b_disalimentabilita: String = null,
                                  t_codice_contratto_vendita: String = null,
                                  t_id_contratto_vend: String = null,
                                  d_data_stipula: Timestamp = null,
                                  t_note: String = null,
                                  d_aggiornamento: Timestamp = null,
                                  n_id_traccia: String = null,
                                  n_id_s_prec: String = null,
                                  tipo_data_inizio: String = null,
                                  tipo_data_fine: String = null,
                                  d_data_rif: Timestamp = null,
                                  t_tipo_fornitura: String = null,
                                  n_indirizzo_fatt: String = null
                                )
