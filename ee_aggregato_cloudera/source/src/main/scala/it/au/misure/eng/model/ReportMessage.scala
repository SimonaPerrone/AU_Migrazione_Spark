package it.au.misure.eng.model

abstract class ReportMessage extends Serializable {
  val cartellaCloud: String = ""
  val nomeFile: String = ""
  val flusso: String = ""
  val ammissibilita: String = ""
  val codiceInamissibilita: String = ""
  val descrizione: String = ""
  var bloccante: String

  val anno: String = ""
  val mese: String = ""
  val giorno: String = ""

  def toStringRow: String

  override def toString: String = s"{ammissibilita: $ammissibilita, bloccante: $bloccante, codiceInamissibilita: $codiceInamissibilita, descrizione: $descrizione}"
}
