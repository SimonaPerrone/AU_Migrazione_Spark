package it.eng.au.portaleConsumi.model.flow.misure

import java.sql.Timestamp

/***
 * Forniture - misure con i dettagli del gruppo per il calcolo del delta
 *
 * Gruppo flusso identifica le misure che devono essere raggruppate per valutarne la priorita' sulla base dei campi
 * gruppo_periodo_competenza: periodo di competenza misura; o un anno mese o un giorno
 * gruppo_priorita': un valore numerico che determina la priorita' della misura (minore il valore maggiore la priorita')
 */
case class FornitureMisureGasGruppoModel(
                                          codice_fiscale: String = null,
                                          p_iva: String = null,
                                          codice_pdr: String = null,
                                          codice_fornitura: String = null,
                                          lettura: Integer = null,
                                          data_lettura: Timestamp = null,
                                          data_caricamento: Timestamp = null,
                                          motivazione: String = null,
                                          flusso: String = null,
                                          gruppo_flusso: Integer = null,
                                          gruppo_periodo_competenza: String = null,
                                          gruppo_priorita: Integer = null,
                                          riempimento: Integer = null,
                                          annomese: String = null,
                                          data_calcolo: String = null
                                        )
