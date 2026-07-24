package it.eng.au.pubblicazione_cce.common

object CostantiCCE {

  val ESITO_OK = "OK"

  val TRATTAMENTO_Y = "Y"

  val RICHIESTA_POD = "POD"
  val RICHIESTA_FILTRO = "FILTRO"

  val RICHIESTA_POD_POD = "POD"
  val RICHIESTA_POD_FILE = "FILE"

  val PROCESSO_P = "P"
  val PROCESSO_PEIN = "Pein"
  val PROCESSO_PEIN_UPPER = PROCESSO_PEIN.toUpperCase()
  val PROCESSO_PR = "PR"
  val PROCESSO_PREIN = "PRein"
  val PROCESSO_PREIN_UPPER = PROCESSO_PREIN.toUpperCase()
  val PROCESSO_CA = "CA"

  val RUOLO_UDD = "UDD"
  val RUOLO_DISTR = "DISTR_RIF"
  val RUOLO_SII = "SII"

  val AMMISSIBILE = "1"
  val INAMMISSIBILE = "0"

  val STATO_CARICATO = "C"
  val STATO_IN_LAVORAZIONE = "IL"
  val STATO_ELABORATO = "E"
  val STATO_NO_CONSUMI = "NC"
  val STATO_NON_AMMISSIBILE = "N"

  // per definizione path di salvataggio file
  val CCE = "CCE"
  val PATH_UDD = "UDD"
  val PATH_ID = "ID"

  // costanti presenti sui dati di input (eventualmente per tests)

  val CCE1 = "CCE1"
  val CCE2 = "CCE2"

  val TENSIONE_ALTISSIMA = "ALTISSIMA"
  val TENSIONE_ALTA = "ALTA"
  val TENSIONE_MEDIA = "MEDIA"
  val TENSIONE_BASSA = "BASSA"

  val ZONA_NORD = "NORD"
  val ZONA_CSUD = "CSUD"
  val ZONA_CALA = "CALA"
  val ZONA_NORE = "NORE"
  val ZONA_SARD = "SARD"
  val ZONA_SICI = "SICI"
  val ZONA_CNOR = "CNOR"
  val ZONA_SUD = "SUD"

}
