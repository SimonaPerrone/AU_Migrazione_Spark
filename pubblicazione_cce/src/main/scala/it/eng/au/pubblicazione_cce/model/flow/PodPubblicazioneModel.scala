package it.eng.au.pubblicazione_cce.model.flow

// Pod estratti da richieste Filtro e POD con attributi necessari per join con misure
case class PodPubblicazioneModel(
                             piva: String, // da richiesta: piva utente richiedente
                             ruolo: String, // da richiesta: UDD, DISTR, SII
                             sessione: String, // da richiesta: CCE
                             processo: String, // da richiesta: P,Pr...
                             anno: String, // da richiesta
                             mese: String, // da richiesta
                             id_richiesta: String, // da richiesta
                             cod_pod: String, // da anagrafica
                             piva_distr: String, // da anagrafica
                             piva_udd: String, // da anagrafica
                             d_inizio_udd: String, // da anagrafica
                             d_fine_udd: String, // da anagrafica,
                             d_data_calc: String, // da calc_track
                             executionid: String // da calc_track
                           )
