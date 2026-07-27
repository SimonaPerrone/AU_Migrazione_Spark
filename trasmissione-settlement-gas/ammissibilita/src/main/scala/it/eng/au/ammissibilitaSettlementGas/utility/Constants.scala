package it.eng.au.ammissibilitaSettlementGas.utility

object Constants {
  val APPLICATION_NAME = "[TSG] Trasmissione Settlement GAS"
  val LOG_NAME = "TSG LOG:"
  val PIVA_SNAM = "10238291008"

  val MANDATORY_ERROR: String = "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati "
  val STRUCTURE_ERROR: String = "Il file non rispetta la struttura prevista - "
  val MISMATCH_INFORMATION_ERROR = "Non è stata rispettata la corrispondenza delle informazioni inviate - "
  val NOT_EXECUTABLE_ERROR = "La richiesta non è eseguibile - "


  val MOTIVAZIONE_FILENAME:String = STRUCTURE_ERROR + "Nome del file non conforme"
  val COD_200 = "200"
  val MOTIVAZIONE_PIVA_RDD = "Non è stata rispettata la corrispondenza delle informazioni inviate " +
    "- Partita IVA presente nel nome file non corrispondente alla Ragione Sociale indicata contestualmente all'accreditamento al SII"
  val MOTIVAZIONE_FILE_INTEGRITY = "Il file utilizzato non rispetta la struttura prevista (file csv corrotto)"

  val COD_209 = "209"
  val MOTIVAZIONE_FILE_HEADER     = STRUCTURE_ERROR + "Intestazione file non corretta"
  val MOTIVAZIONE_FILE_EXTENSION  = STRUCTURE_ERROR + "Sono ammessi solo file csv o xml"
  val MOTIVAZIONE_DATE_MISMATCH   = MISMATCH_INFORMATION_ERROR + "Il campo data non risulta coerente " +
    "con il mese/anno della nomenclatura file"
  val MOTIVAZIONE_ALREADY_TRANSMITTED = STRUCTURE_ERROR + "Presente altro file con stessa nomenclatura"
  val MOTIVAZIONE_ALREADY_TRANSMITTED2 = NOT_EXECUTABLE_ERROR + "Presente altro file con stessa nomenclatura"


  val MOTIVAZIONE_DATE_FORMAT = "Formato data non valido"
  val MOTIVAZIONE_DATE_EXISTANCE = MOTIVAZIONE_DATE_FORMAT + ", valore non ammissibile"
  val COD_224 = "224"

  val MOTIVAZIONE_GIORNO_RIF = MISMATCH_INFORMATION_ERROR + "il campo GIORNO_RIFERIMENTO non risulta coerente con l'anno termico"

  //val MOTIVAZIONE_MANDATORY_FIELDS = MANDATORY_ERROR // DEVO AGGIUNGERE DOPO C1_XX QUINDI FAREMO CONTESTUALMENTE AL CHECK.

  val MOTIVAZIONE_DATE  = MANDATORY_ERROR + "(DATA non presente)"
  val MOTIVAZIONE_ID_REG_CLIM_MANDATORY = MANDATORY_ERROR + "(ID_REG_CLIM non presente)"
  val MOTIVAZIONE_ID_REG_CLIM_COMPLIANCE = "ID_REG_CLIM non conforme"
  val MOTIVAZIONE_WKR_DEFINED = MANDATORY_ERROR + "(WKR non presente)"
  val MOTIVAZIONE_WKR_FORMAT = MANDATORY_ERROR + "(WKR non corretto)"
  val MOTIVAZIONE_COD_REMI_MANDATORY = MANDATORY_ERROR + "(COD_REMI non presente)"
  val MOTIVAZIONE_QKRIUD_MANDATORY = MANDATORY_ERROR + "(QKRIUD non presente)"
  val MOTIVAZIONE_COD_REMI_FORMAT = MANDATORY_ERROR + "(COD_REMI non corretto)"
  val MOTIVAZIONE_QKRIUD_FORMAT = MANDATORY_ERROR + "(QKRIUD non corretto)"
  val MOTIVAZIONE_COD_REMI_EXISTANCE = MANDATORY_ERROR + "Remi inesistente"

  val COD_004 = "004"
  val COD_015 = "015"
  val COD_005 = "005"

  val MOTIVAZIONE_FIELDS_NUMBER = "Il template (formato file e/o tracciato) utilizzato non è congruo"
  val COD_001 = "001"

  val CSV_DELIMITER = ";"
  val DATA_COMP_FORMAT = "dd/MM/yyyy"
  val ID_REG_CLIM_VALUES_Complete = List(("11","Torino,Caselle"),("18","Bologna Borgo Panigale"),
    ("13","Milano Linate"),("14","Bolzano"),("17","Genova Sestri"),("15","Venezia Tessera"),
    ("16","Trieste"),("21","Falconara"),("24","Campobasso"),("19","Firenze"),("22","Roma"),
    ("20","Perugia Sant'Egidio"),("27","Potenza"),("25","Napoli"),("23","Pescara"),
    ("26","Bari"),("28","Reggio Calabria"),("29","Catania Fontanarossa"))

  val ID_REG_CLIM_VALUES_2 = List(("11"->"Torino Caselle"),("18"->"Bologna Borgo Panigale"),
    ("13"->"Milano Linate"),("14"->"Bolzano"),("17"->"Genova Sestri"),("15"->"Venezia Tessera"),
    ("16"->"Trieste"),("21"->"Falconara"),("24"->"Campobasso"),("19"->"Firenze"),("22"->"Roma"),
    ("20"->"Perugia Sant'Egidio"),("27"->"Potenza"),("25"->"Napoli"),("23"->"Pescara"),
    ("26"->"Bari"),("28"->"Reggio Calabria"),("29"->"Catania Fontanarossa"))

  val ID_REG_CLIM_VALUES = List("11","18","13","14","17","15","16","21","24","19","22","20",
    "27","25","23","26","28","29")

  val YEARMONTH_MIN = "190001"
  val YEARMONTH_MAX = "210012"
}
