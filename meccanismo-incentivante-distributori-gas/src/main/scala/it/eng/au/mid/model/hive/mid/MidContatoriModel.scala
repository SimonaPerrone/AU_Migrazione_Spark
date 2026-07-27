package it.eng.au.mid.model.hive.mid

import java.sql.Date

/** *
 *
 * @param pdr                          codice PDR
 * @param contatore                    contatore anomalie
 * @param stato                        V(valido)/I(invalido)/F(forzato)
 * @param treatment                    trattamento PDR (Y/G/M)
 * @param data_tracciatura             data di calcolo
 * @param processo_tracciatura         SBG/AGG
 * @param sessione_tracciatura         sessione completa da daily (e.g. AGG_S1_FIN)
 * @param causale_tracciatura          E presente in esclusi, I presente in incoerenti, EI presente in entrambi
 * @param tipo_calcolo                 ordinario/straordinario (SBG sempre ordinario)
 * @param executionid_daily_consumption  executionid daily consumption
 * @param executionid_tracciatura_prev executionid MID precedente
 * @param annomese                     annomese calcolato
 * @param executionid_tracciatura      execution_id processo
 */
case class MidContatoriModel(
                              pdr: String = null,
                              contatore: Int = 0, // da calcolo o forzatura
                              stato: String = null, // da calcolo o forzatura
                              treatment: String = null, // da anagrafica
                              data_tracciatura: Date = null, // da processo
                              processo_tracciatura: String = null, // da processo
                              sessione_tracciatura: String = null, // da anagrafica
                              causale_tracciatura: String = null, // da forzature
                              tipo_calcolo: String = null, // da processo
                              executionid_daily_consumption: java.lang.Long = null, // da anagrafica
                              executionid_tracciatura_prev: java.lang.Long = null, // da calcolo
                              annomese: String = null, // da processo
                              executionid_tracciatura: java.lang.Long = null // da processo
                            )
