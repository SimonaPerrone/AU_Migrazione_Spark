package it.eng.au.ccgPubblicazione.model.request

import it.eng.au.ccgPubblicazione.utility.Constants.{AMMISSIBILITA_PDR, AMMISSIBILITA_SI_1}

case class RequestPdr(
                       N_ID_RICHIESTA: String = null,
                       T_TIPO: String = null,
                       T_SERVIZIO: String = null,
                       T_PROCESSO: String = null,
                       T_ANNO: String = "2022",
                       T_MESE: String = null,
                       T_RUOLO: String = null,
                       T_PIVA: String = null,
                       T_CODICE_PDR: String = null,
                       B_AMMISSIBILITA: String = AMMISSIBILITA_SI_1,
                       T_COD_CAUSALE: String = null,
                       T_MOTIVAZIONE: String = null,
                       T_NOME_FILE: String = null,
                       T_TIPO_AMM: String = AMMISSIBILITA_PDR
                     )
