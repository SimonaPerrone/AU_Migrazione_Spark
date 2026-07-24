package it.eng.au.pubblicazioneIndennizzi.model

import java.sql.Timestamp

case class CIGPubblicazioneIndennizzi(
                                       input_table_execution_id: String = "",
                                       operation_name: String = "",
                                       base_name: String = "",
                                       path_name: String = "",
                                       load_date: Timestamp = Timestamp.valueOf("2022-01-01"),
                                       annomese: String = "" // For partitioning
                                     )