package it.eng.au.ccgPubblicazione.args

import it.eng.au.ccgPubblicazione.utility.Constants.ALL
import org.joda.time.DateTime

case class FlowArgsConfig(
                           pathToProperties: String = null,
                           session: String = "AGG", //AGG/CCG/CDP_FIN/CDP_RIC
                           dataRichiesta: String = null,
                           tipo: String = ALL //PDR/FILTRO/ALL
                         )
