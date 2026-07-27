package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.{Date, Timestamp}

case class RcugasPdrMisuratorePModel(
                                      n_id_pdr_misuratore: String = null,
                                      n_id_pdr: String = null,
                                      t_matricola_misuratore: String = null,
                                      t_tipo_misuratore: String = null,
                                      t_telegestito: String = null,
                                      n_coeff_correzione: String = null,
                                      t_classe_misuratore: String = null,
                                      t_access_misuratore: String = null,
                                      n_num_cifre_misuratore: String = null,
                                      t_anno_fabbric_misuratore: String = null,
                                      t_data_inst_misuratore: Date = null,
                                      t_misuratore_integrato: String = null,
                                      t_presenza_convertitore: String = null,
                                      t_matricola_convertitore: String = null,
                                      n_num_cifre_convertitore: String = null,
                                      t_anno_fabbric_convertitore: String = null,
                                      t_data_inst_convertitore: Timestamp = null,
                                      n_lettura_convertitore: String = null,
                                      t_note: String = null,
                                      d_aggiornamento: Timestamp = null,
                                      n_id_traccia: String = null,
                                      n_id_s_prec: String = null,
                                      d_data_rif: Timestamp = null
                                    )
