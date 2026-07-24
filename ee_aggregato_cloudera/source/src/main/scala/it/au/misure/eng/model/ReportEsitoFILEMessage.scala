package it.au.misure.eng.model

import it.au.misure.eng.utility.Constants._

case class ReportEsitoFILEMessage(
                                 override val cartellaCloud: String = "",
                                 override val nomeFile: String = "",
                                 override val flusso: String = "",
                                 override val ammissibilita: String = FILE,
                                 override var bloccante: String = BLOCCANTE,
                                 override val codiceInamissibilita: String = "",
                                 override val descrizione: String = "",
                                 override val anno: String = "",
                                 override val mese: String = "",
                                 override val giorno: String = ""
                               ) extends ReportMessage {

  def toStringRow: String = List(cartellaCloud, nomeFile, flusso, ammissibilita, bloccante, codiceInamissibilita, descrizione).mkString(CSV_REPORT_SEPARATOR)
}

object ReportEsitoFILEMessage {
  def header: String = List("CartellaCloud","Nomefile","Flusso","Ammissibilità","Bloccante","Codice_Inammissibilità","Descrizione").mkString(CSV_REPORT_SEPARATOR)
}

