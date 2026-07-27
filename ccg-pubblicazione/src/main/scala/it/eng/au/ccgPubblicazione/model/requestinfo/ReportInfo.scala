package it.eng.au.ccgPubblicazione.model.requestinfo

case class ReportInfo(
                       idRichiesta: String,
                       pathAmmissibilita: String,
                       fileAmmissibilita: String,
                       writeError: Boolean
                     )