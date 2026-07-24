package it.eng.au.pubblicazione_cce.args

import java.time.LocalDate

case class Args(
                 dataRichieste: LocalDate = null,
                 flow: String = null,
                 pathToProperties: String = null
               )

