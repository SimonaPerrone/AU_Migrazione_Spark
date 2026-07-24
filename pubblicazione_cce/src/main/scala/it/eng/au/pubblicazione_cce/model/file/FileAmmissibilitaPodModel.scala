package it.eng.au.pubblicazione_cce.model.file

case class FileAmmissibilitaPodModel(
                                      id_richiesta: String,
                                      timestamp: String,
                                      pod: String,
                                      ammissibilita: String, // 0 se non passa i controlli, altrimenti 1
                                      codice_inammissibilita: String,
                                      descrizione: String
                                    )
