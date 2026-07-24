package it.eng.au.pubblicazioneRendiconti.model

import java.sql.Timestamp

case class ReportPubblicazioneRzg2(
                    input_table_execution_id: String,
                    operation_name: String,
                    base_name: String,
                    path_name: String,
                    load_date: Timestamp,
                    annomese: String
                  )
