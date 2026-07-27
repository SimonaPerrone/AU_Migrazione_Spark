package it.au.misure.ee_switching.model.schema.hive

import java.sql.Timestamp

case class ReportEntry (
                         piva_distributore: String = null,
                         piva_utente: String = null,
                         pod: String = null,
                         nome_flusso: String = null,
                         percorso_file: String = null,
                         validation_output: String = null,
                         d_creazione: Timestamp = null,
                         annomese_sw: String = null
                       )

