package it.eng.au.portaleConsumi.model.hive.misuregas

import java.sql.Timestamp

/*
 * Classe per le informazioni necessarie post associazione misure con forniture
 */
case class FornitureMisureGasModel(
                                    codice_fiscale: String = null,
                                    p_iva: String = null,
                                    codice_pdr: String = null,
                                    codice_fornitura: String = null,
                                    lettura: Integer = null,
                                    data_lettura: Timestamp = null,
                                    motivazione: String = null,
                                    data_caricamento: Timestamp = null,
                                    annomese: String = null,
                                    flusso: String = null
                                  )
