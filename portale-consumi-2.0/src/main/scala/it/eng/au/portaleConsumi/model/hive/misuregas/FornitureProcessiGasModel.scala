package it.eng.au.portaleConsumi.model.hive.misuregas

import java.sql.Timestamp

/**
 * Classe che contiene tutte le informazioni per la struttura della Fornitura Gas
 */
case class FornitureProcessiGasModel(
                                      hashcode: String = null,
                                      codice_fiscale: String = null,
                                      codice_pdr: String = null,
                                      nome: String = null,
                                      cognome: String = null,
                                      p_iva: String = null,
                                      ragione_sociale: String = null,
                                      cap: String = null,
                                      categoria_uso: String = null,
                                      civico: String = null,
                                      classe_misuratore: String = null,
                                      codice_fornitura: String = null,
                                      coefficiente_conversione: String = null,
                                      comune: String = null,
                                      data_inizio_fornitura: Timestamp = null,
                                      data_fine_fornitura: Timestamp = null,
                                      data_aggiornamento: Timestamp = null,
                                      matricola_misuratore: String = null,
                                      nazione: String = null,
                                      nome_strada: String = null,
                                      p_iva_cc: String = null,
                                      provincia: String = null,
                                      ragione_sociale_cc: String = null,
                                      ragione_sociale_distributore: String = null,
                                      residente: String = null,
                                      tipo_fornitura: String = null,
                                      tipo_pdr: String = null,
                                      toponimo_Indirizzo: String = null,
                                      data_inizio_processo_gdm: Timestamp = null,
                                      data_fine_processo_gdm: Timestamp = null,
                                      data_di_decorrenza_gdm: Timestamp = null,
                                      id_processo_gdm: String = null,
                                      in_corso_gdm: String = null,
                                      note_gdm: String = null,
                                      tipo_processo_gdm: String = null,
                                      data_inizio_processo_switch: Timestamp = null,
                                      data_fine_processo_switch: Timestamp = null,
                                      data_di_decorrenza_switch: Timestamp = null,
                                      id_processo_switch: String = null,
                                      in_corso_switch: String = null,
                                      note_switch: String = null,
                                      tipo_processo_switch: String = null,
                                      codice_offerta: String = null,
                                      cliente_vulnerabile: String = null,
                                      data_calcolo: String = null
                                    )
