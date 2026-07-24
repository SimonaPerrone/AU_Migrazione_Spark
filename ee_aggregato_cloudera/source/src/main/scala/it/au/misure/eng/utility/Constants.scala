package it.au.misure.eng.utility

object Constants {
  val _1G = "1G"
  val _2G = "2G"

  val CSV_REPORT_SEPARATOR = ";"

  val FILE = "FILE"
  val POD = "POD"

  val BLOCCANTE = "BLOCCANTE"
  val NON_BLOCCANTE = "NON BLOCCANTE"
  val OK = "OK"

  val YES  = "S"
  val NO = "N"

  val COD906 = "906"
  val COD010 = "010"
  val COD904 = "904"
  val COD914 = "914"
  val COD919 = "919"

  val COD967 = "967"

  val ERROR_FILE_STRUCTURE = "Il file non rispetta la struttura prevista "
  val ERROR_FILE_ALREADY_TRANSMITTED = "File già trasmesso "
  val ERROR_FILE_FUTURE = "I dati del file si riferiscono ad un mese futuro o antecedente all’attuale per più di 5 anni"
  val ERROR_COMPILATION_FIELD = "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati "

  val ERROR_CONTRACT_CODE = "Codice Contratto Dispacciamento strutturalmente scorretto "
  val ERROR_POD_COMPETENCE = "Il POD non è di competenza "
}
