package it.eng.au.pubblicazione_cce.model.cce

import java.sql.Timestamp

case class CceEsitoModel(
                          n_id_richiesta: String = null, // da richiesta
                          t_ruolo: String = null, // da richiesta
                          t_path: String = null, // da properties + nome file
                          t_file_esito: String = null, // calcolato: lista dei file zip prodotti in fase di elaborazione
                          t_file_ammissibilita: String = null, // calcolato: lista dei file csv prodotti in fase di elaborazione ammissibilità (solo richiesta pod)
                          // calcolato [C (caricata), IL (in lavorazione), IE (errore interno, stato solo webapp),
                          // E (elaborato), NC (no consumi), N (non ammissibile)]
                          t_stato: String = null, // commento sopra ^
                          t_operation_name: String = null, // tipo richiesta
                          t_number_file_zip: Integer = null, // numero di Zip (+csv?) di cui si compone la pubblicazione
                          execution_id_input_read: String = null, // executionid misure
                          d_data_esito: Timestamp = null, // data giorno di calcolo
                          tipo_richiesta: String = null, // da richiesta [POD, FILTRO]
                          n_executionid: String = null, // calcolato
                          d_data_richiesta: String = null // da richiesta
                        )
