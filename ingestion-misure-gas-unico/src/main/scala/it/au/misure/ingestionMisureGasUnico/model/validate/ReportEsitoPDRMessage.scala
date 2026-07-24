package it.au.misure.ingestionMisureGasUnico.model.validate

import it.au.misure.ingestionMisureGasUnico.utility.Constants._

case class ReportEsitoPDRMessage(
                                  override val cartellaCloud: String = "",
                                  override val nomeFile: String = "",
                                  override val flusso: String = "",
                                  pdr: String = "",
                                  override val ammissibilita: String = PDR,
                                  override var bloccante: String = OK,
                                  override val codiceInamissibilita: String = "",
                                  override val descrizione: String = "",
                                  override val anno: String = "",
                                  override val mese: String = "",
                                  override val giorno: String = ""
                               ) extends ReportMessage {

  def toStringRow: String = List(cartellaCloud, nomeFile, pdr, ammissibilita, bloccante, codiceInamissibilita, descrizione).mkString(CSV_REPORT_SEPARATOR)
}
object ReportEsitoPDRMessage {
  def header: String = List("CartellaCloud","Nomefile","PDR","Ammissibilità","Bloccante","Codice_Inammissibilità","Descrizione").mkString(CSV_REPORT_SEPARATOR)
}

