package it.au.misure.eng.model
import it.au.misure.eng.utility.Constants._

case class ReportEsitoPODMessage(
                                 override val cartellaCloud: String = "",
                                 override val nomeFile: String = "",
                                 pod: String = "",
                                 override val flusso: String = "",
                                 override val ammissibilita: String = "",
                                 override var bloccante: String = OK,
                                 override val codiceInamissibilita: String = "",
                                 override val descrizione: String = "",
                                 override val anno: String = "",
                                 override val mese: String = "",
                                 override val giorno: String = ""
                               ) extends ReportMessage {

  def toStringRow: String = List(cartellaCloud, nomeFile, pod, flusso, ammissibilita, bloccante, codiceInamissibilita, descrizione).mkString(CSV_REPORT_SEPARATOR)
}
object ReportEsitoPODMessage {
  def header: String = List("CartellaCloud","Nomefile","POD","Flusso","Ammissibilità","Bloccante","Codice_Inammissibilità","Descrizione").mkString(CSV_REPORT_SEPARATOR)
}
