package it.eng.au.pubblicazione_cce.model.file

case class FileElencoFlussiCaModel(
                                  //file name
                                  piva: String,
                                  ruolo: String, // da richiesta: UDD, DISTR, SII
                                  sessione: String, //CCE
                                  processo: String, //P,Pr...
                                  anno: String,
                                  timestamp: String,
                                  id_richiesta: String,
                                  //contenuto
                                  pod: String,
                                  path_cloud: String
                                )
