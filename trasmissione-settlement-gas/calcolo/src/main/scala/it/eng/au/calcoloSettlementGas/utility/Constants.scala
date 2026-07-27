package it.eng.au.calcoloSettlementGas.utility

object Constants {
  // TODO: Probabilmente sarebbe meglio differenziare i due application name in Calcolo ed Ammissibilita.
  val APPLICATION_NAME = "[TSG] Trasmissione Settlement GAS"
  val LOG_NAME = "TSG LOG:"

  val ID_REG_CLIM_VALUES_Complete = List(("11","Torino,Caselle"),("18","Bologna Borgo Panigale"),
    ("13","Milano Linate"),("14","Bolzano"),("17","Genova Sestri"),("15","Venezia Tessera"),
    ("16","Trieste"),("21","Falconara"),("24","Campobasso"),("19","Firenze"),("22","Roma"),
    ("20","Perugia Sant'Egidio"),("27","Potenza"),("25","Napoli"),("23","Pescara"),
    ("26","Bari"),("28","Reggio Calabria"),("29","Catania Fontanarossa"))
  val CSV_DELIMITER = ";"
}
