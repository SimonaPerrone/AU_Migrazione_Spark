package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasClientefinalePModel(
                                      n_id_cliente: String = null,
                                      t_codice_fiscale: String = null,
                                      t_partita_iva: String = null,
                                      t_nome: String = null,
                                      t_cognome: String = null,
                                      t_ragione_sociale: String = null,
                                      t_note: String = null,
                                      t_dettaglio_cf: String = null,
                                      t_dettaglio_piva: String = null,
                                      t_sede_legale: String = null,
                                      d_aggiornamento: Timestamp = null,
                                      n_id_traccia: String = null,
                                      n_id_s_prec: String = null,
                                      t_dettaglio_anacli: String = null,
                                      d_data_rif: Timestamp = null,
                                      t_codice_ateco: String = null,
                                      b_cf_straniero: String = null,
                                      b_persona_fisica: String = null,
                                      t_telefono: String = null,
                                      t_email: String = null
                                    )
