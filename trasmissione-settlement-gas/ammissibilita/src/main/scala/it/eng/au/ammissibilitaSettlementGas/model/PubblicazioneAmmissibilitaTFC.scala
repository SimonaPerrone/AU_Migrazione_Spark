package it.eng.au.ammissibilitaSettlementGas.model

case class PubblicazioneAmmissibilitaTFC(
                                        n_id_tsg2_file: String,
                                        cartella_cloud: String,
                                        csv_file_name: String,
                                        ammissibilita_file_name: String,
                                        verifica_amm: String,
                                        cod_causale: String,
                                        motivazione: String,
                                        numero_riga: String,
                                        data_amm: String,
                                        annomese: String
                                        ) extends Serializable
