package it.eng.au.pubblicazione_cce.model.file

case class FileConsumiModel(
                             // file name
                             piva: String, // da richiesta: piva utente richiedente
                             ruolo: String, // da richiesta: UDD, DISTR, SII
                             sessione: String, // da richiesta: CCE
                             processo: String, // da richiesta: P,Pr...
                             anno: String, // da richiesta
                             mese: String, // da richiesta
                             timestamp: String, //ts di calcolo
                             id_richiesta: String, // da richiesta

                             //contenuto
                             data: String, // misure: data misura
                             cod_pod: String, // misure
                             piva_distr: String, // da anagrafica
                             piva_udd: String, // da anagrafica
                             h01: String, // misure
                             h02: String,
                             h03: String,
                             h04: String,
                             h05: String,
                             h06: String,
                             h07: String,
                             h08: String,
                             h09: String,
                             h10: String,
                             h11: String,
                             h12: String,
                             h13: String,
                             h14: String,
                             h15: String,
                             h16: String,
                             h17: String,
                             h18: String,
                             h19: String,
                             h20: String,
                             h21: String,
                             h22: String,
                             h23: String,
                             h24: String,
                             h25: String,
                             data_aggiornamento: String, // da calc_track

                             // per elenco flussi e esito
                             nome_file: String,
                             executionid: String
                           )
