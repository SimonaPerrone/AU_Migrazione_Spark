package it.eng.au.pubblicazione_cce.model.cce

import java.sql.Timestamp

case class CceEsitoViewModel(
                          n_id_richiesta: String = null, // da richiesta
                          t_path: String = null, // da properties + nome file
                          t_file_esito: String = null, // calcolato: lista dei file zip prodotti in fase di elaborazione
                          t_file_ammissibilita: String = null, // calcolato: lista dei file csv prodotti in fase di elaborazione ammissibilità (solo richiesta pod)
                          // calcolato [C (caricata), IL (in lavorazione), IE (errore interno, stato solo webapp),
                          // E (elaborato), NC (no consumi), N (non ammissibile)]
                          t_stato: String = null, // commento sopra ^
                          d_data_esito: Timestamp = null // data giorno di calcolo
                        )
