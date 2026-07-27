package it.eng.au.ccgPubblicazione.model.requestinfo

case class RequestInfo(
                        idrichiesta: String,
                        piva: String,
                        pdrReport: Option[(String, String, Boolean)],
                        tipoAmmissibilita: Option[String],
                        infoOption:  Option[(String, Int)]
                      )
