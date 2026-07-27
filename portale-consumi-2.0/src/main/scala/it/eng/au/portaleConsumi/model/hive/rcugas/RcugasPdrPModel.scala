package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasPdrPModel(
                            n_id_pdr: String = null,
                            t_codice_pdr: String = null,
                            t_cod_tipo_pdr: String = null,
                            t_codice_istat: String = null,
                            t_note: String = null,
                            d_aggiornamento: Timestamp = null,
                            n_id_traccia: String = null,
                            n_id_s_prec: String = null,
                            n_id_indirizzo: String = null,
                            d_data_rif: Timestamp = null,
                            t_disalimentabilita: String = null,
                            t_accesso_ui: String = null
                          )
