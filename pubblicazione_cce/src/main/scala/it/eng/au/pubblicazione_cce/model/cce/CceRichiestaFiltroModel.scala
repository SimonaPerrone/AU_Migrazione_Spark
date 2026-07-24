package it.eng.au.pubblicazione_cce.model.cce

import java.sql.Timestamp

case class CceRichiestaFiltroModel(
                                    n_id_richiesta: String,
                                    t_tipo: String, // [POD, FILTRO]
                                    t_servizio: String, // [CCE1, CCE2] determina posizione di salvataggio file prodotti
                                    t_processo: String, // [P, Pr, Pein, PRein, CA]
                                    d_data_richiesta: Timestamp,
                                    t_anno: String,
                                    t_mese: String,
                                    t_ruolo: String, // [UDD_EEL; DISTR_ALL; SII] filtri per letture richieste
                                    t_piva: String,
                                    t_tensione: String,
                                    t_zona: String,
                                    t_tipo_pod: String,
                                    t_piva_udd: String,
                                    t_piva_id: String,
                                    t_codice_terna: String,
                                    t_tariffa: String,
                                    sqoop_date: Timestamp,
                                    partition_request_date: String
                        )
