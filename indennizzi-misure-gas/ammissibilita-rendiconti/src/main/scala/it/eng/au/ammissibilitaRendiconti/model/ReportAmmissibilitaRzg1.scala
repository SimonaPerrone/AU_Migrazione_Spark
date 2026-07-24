package it.eng.au.ammissibilitaRendiconti.model

import java.sql.Timestamp

case class ReportAmmissibilitaRzg1(
                                    cartella_cloud: String,
                                    zip_file_name: String,
                                    zip_last_modified_date: Long,
                                    csv_file_name: String,
                                    cartella_cloud_ammissibilita: String,
                                    ammissibilita_file_name: String,
                                    ammissibilita: Boolean,
                                    codice: String,
                                    descrizione: String,
                                    data_creazione: Timestamp,
                                    annomese: String
                                  ) extends Serializable
