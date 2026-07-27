package it.eng.au.mid.model.file.pubblicazione

// Contenuto del file CSV e metadati utilizzati per nome file e salvataggio in tabella finale
// valido per MID1 e MID2
case class MidCsvModel(
                         piva_distr: String = null,
                         piva_udd: String = null,
                         riga_file: String = null, // contenuto riga
                         progressivo_file: Int // progressivo, numero file
                       )