package it.eng.au.pubblicazione_cce.model.file

case class FileAmmissibilitaFileModel(
                                       servizio: String,
                                       piva: String,
                                       id_richiesta: String,
                                       file: String,
                                       ammissibilita: String, // 0 se non passa i controlli, altrimenti 1
                                       codice_inammissibilita: String, // vedere documentazione
                                       descrizione: String
                                     )
