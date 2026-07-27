package it.eng.au.portaleConsumi.model.flow.misure

import java.sql.Timestamp

/** *
 * Classe contenente tutti i dettagli delle misure per lo storico
 */
case class FornitureMisureGasArricchiteModel(
                                              codice_fiscale: String = null,
                                              codice_pdr: String = null,
                                              codice_fornitura: String = null,
                                              lettura: Integer = null,
                                              flusso: String = null,
                                              gruppo_flusso: Integer = null,
                                              data_lettura: Timestamp = null,
                                              data_caricamento: Timestamp = null,
                                              motivazione: String = null,
                                              delta_misure: Integer = null,
                                              usata_per_calcolo: Integer = null, //se usata nel salvataggio finale
                                              riempimento: Integer = null, // 0 misura letta, 1 fill inizio fornitura, 2 calcolo mensile da giornaliero
                                              cod_pdr: String = null,
                                              data_calcolo: String = null,
                                              annomese: String = null
                                            )
