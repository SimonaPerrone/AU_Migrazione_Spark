package it.eng.au.portaleConsumi.model.hive.switch_gas

import java.sql.Timestamp


case class PrtVtg6PModel(
                          n_id_vtg6: String = null,
                          n_id_vtg: String = null,
                          n_id_pratica: String = null,
                          n_id_utente: String = null,
                          t_codice_pdr: String = null,
                          t_matr_mis: String = null,
                          d_data_att_contr: String = null,
                          n_vol_annuo_sost: String = null,
                          t_classe_gruppo_mis: String = null,
                          t_cifre_mis: String = null,
                          t_segn_mis_sost: Integer = null,
                          t_pre_conv: String = null,
                          t_gruppo_mis_int: String = null,
                          n_coeff_corr: String = null,
                          t_matr_conv: String = null,
                          t_cifre_conv: String = null,
                          t_segn_conv: String = null,
                          d_data_mis_eff: String = null,
                          t_segn_mis_eff: Integer = null,
                          t_segn_conv_eff: String = null,
                          t_note: String = null,
                          t_tipo_lettura: String = null,
                          b_copiato_tmg_misure: String = null,
                          cod_flusso: String = null,
                          d_caricamento: Timestamp = null,
                          d_data_mis_eff_ts: Timestamp = null
                        )
