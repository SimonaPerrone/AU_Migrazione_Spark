package it.eng.au.ccgPubblicazione.model.request

import java.sql.Timestamp

case class RequestEsito(
                         N_ID_RICHIESTA: Long,
                         T_PATH: String,
                         T_FILE_ESITO: String,
                         T_FILE_AMMISSIBILITA: Option[String],
                         T_STATO: String, // Chiusa / Non Ammissibile / no consumi
                         T_OPERATION_NAME: String,
                         T_NUMBER_FILE_ZIP: Int,
                         EXECUTION_ID_INPUT_READ: String,
                         D_DATA_ESITO: Timestamp,
                         TIPO_RICHIESTA: String,
                         D_DATA_RICHIESTA: String,
                         sessione: String,
                         executionid: Long
                       )
