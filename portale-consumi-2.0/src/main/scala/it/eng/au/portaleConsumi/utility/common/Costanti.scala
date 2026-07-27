package it.eng.au.portaleConsumi.utility.common

case object Costanti{

  // nomi processi
  val PROCESSO_3M = "3M"
  val PROCESSO_33M = "33M"

  // tipi flusso letture
  val TML = "tml"
  val RML = "rml"
  val TGL = "tgl"
  val RGL = "rgl"
  val TAV = "tav"
  val TAL = "tal"
  val VTG = "vtg"
  val RMV = "rmv"
  val _MAN = "man" // inserimento riempimento

  // categorie di raggruppamento nella collezione MongoDB
  val CATEGORIA_MISURE_AF =  "misure_altre_frequenze"
  val CATEGORIA_MISURE_MENSILI =  "misure_mensili"
  val CATEGORIA_MISURE_GIORNALIERE =  "misure_giornaliere"
  val CATEGORIA_MISURE_AUTOLETTURA =  "autoletture"
  val CATEGORIA_MISURE_VOLTURA =  "volture"
  val CATEGORIA_RIEMPIMENTO = ""

  // descrizione tipo lettura usato in MongoDB
  val TIPO_LETTURA_PERIODICA =  "Lettura Periodica"
  val TIPO_LETTURA_RETTIFICA =  "Lettura di Rettifica"
  val TIPO_LETTURA_RETTIFICA_VOLTURA =  "Lettura di Rettifica Voltura"
  val TIPO_MISURE_AUTOLETTURA = "Autoletture"
  val TIPO_MISURE_VOLTURA =  "Lettura Voltura"
  val TIPO_RIEMPIMENTO =  ""

  // gruppi per calcolare le rettifiche a seconda del tipo di misura
  val GRUPPO_MISURE_MENSILI = 1
  val GRUPPO_MISURE_GIORNALIERE = 2
  val GRUPPO_MISURE_VOLTURE = 3
  val GRUPPO_MISURE_AUTOLETTURE = 4

  // raggruppamenti per calcolare il delta tra le misure provenienti dai diversi gruppi
  val GRUPPO_DELTA_MENSILI = 1
  val GRUPPO_DELTA_GIORNALIERI = 2
  val GRUPPO_DELTA_AUTOLETTURE = 3

  // descrizione motivaizone rettifiche
  val MOTIVAZIONE1 = "Misura che sostituisce una stima precedente."
  val MOTIVAZIONE2 = "Misura che sostituisce una misura fornita precedentemente errata."
  val MOTIVAZIONE3 = "Misura fornita precedentemente per errore."
  val MOTIVAZIONE4 = "Ricostruzione per frode."
  val MOTIVAZIONE5 = "Ricostruzione per malfunzionamento misuratore."

}
