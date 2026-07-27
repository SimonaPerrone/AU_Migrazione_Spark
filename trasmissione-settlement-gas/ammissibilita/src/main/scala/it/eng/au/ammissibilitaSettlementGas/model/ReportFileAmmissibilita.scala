package it.eng.au.ammissibilitaSettlementGas.model

case class ReportFileAmmissibilita(
                                    cartella_cloud: String,
                                    csv_file_name: String,
                                    last_modified: Long,
                                    ammissibilita_file_name: String
                                  ) extends Serializable
