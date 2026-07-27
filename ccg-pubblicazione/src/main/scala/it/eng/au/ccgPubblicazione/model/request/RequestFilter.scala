package it.eng.au.ccgPubblicazione.model.request

case class RequestFilter(
                          N_ID_RICHIESTA: String = null,
                          T_TIPO: String = null,
                          T_SERVIZIO: String = null,
                          T_PROCESSO: String = null,
                          T_ANNO: String = null,
                          T_MESE: String = null,
                          T_RUOLO: String = null,
                          T_PIVA: String = null,
                          T_COD_REMI: String = null,
                          T_INCOERENTI: String = null,
                          T_TRATTAMENTO: String = null,
                          T_PIVA_UDD: String = null,
                          T_PIVA_UDB: String = null,
                          T_PIVA_ID: String = null,
                          T_CODPROFSTD: String = null
                     )
