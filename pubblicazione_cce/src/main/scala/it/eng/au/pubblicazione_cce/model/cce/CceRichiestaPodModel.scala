package it.eng.au.pubblicazione_cce.model.cce

import java.sql.Timestamp

case class CceRichiestaPodModel(
                                 n_id_richiesta: String,
                                 t_servizio: String, // [CCE1, CCE2] determina posizione di salvataggio file prodotti
                                 t_processo: String, // [P, Pr, Pein, PRein, CA]
                                 d_data_richiesta: Timestamp,
                                 t_anno: String,
                                 t_mese: String,
                                 t_ruolo: String, // [UDD_EEL; DISTR_ALL; SII] filtri per letture richieste
                                 t_piva: String,
                                 t_codice_pod: String,
                                 b_ammissibilita: String, // [0,1]
                                 t_cod_causale: String, // codice inammissibilità
                                 t_motivazione: String, // descrizione inammissibilità
                                 t_nome_file: String, // nome file
                                 t_tipo_amm: String, // [FILE, POD]
                                 sqoop_date: Timestamp, // ts import sqoop
                                 partition_request_date: String // d_data_richiesta ?
                               )
