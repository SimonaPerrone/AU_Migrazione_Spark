package it.eng.au.pubblicazione_cce.model.file

case class FileConsumiCaExplodedModel(
                             // file name
                             piva: String, // da richiesta: piva utente richiedente
                             ruolo: String, // da richiesta: UDD, DISTR, SII
                             sessione: String, // da richiesta: CCE
                             processo: String, // da richiesta: CA
                             anno: String, // da richiesta
                             timestamp: String, //ts di calcolo
                             id_richiesta: String, // da richiesta

                             //contenuto
                             cod_pod: String, // misure
                             piva_distr: String, // da anagrafica
                             piva_udd: String, // da anagrafica
                             ca: Double,
                             data_aggiornamento: String, // da calc_track

                             // per elenco flussi e esito
                             nome_file: String, //nome file
                             executionid: String
                           )
