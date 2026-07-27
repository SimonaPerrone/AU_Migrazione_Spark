package it.eng.au.mid.model.hive.mid

import java.sql.Date

case class MidAggregatoreInfoModel(
                                    operation_name: String = null, // MID1, MID2
                                    nome_file: String = null,
                                    path: String = null,
                                    tipo_dest: String = null, // costante ID per MID1, CSEA per MID2
                                    piva_dest: String = null, // uguale a piva ID per MID1, costante piva CSEA per MID2
                                    piva_id_file: String = null,
                                    piva_udd_file: String = null, // sempre null
                                    data_caricamento: Date = null,
                                    executionid_mid_dettaglio: Long = 0L,
                                    executionid: Long = 0L
                                  )