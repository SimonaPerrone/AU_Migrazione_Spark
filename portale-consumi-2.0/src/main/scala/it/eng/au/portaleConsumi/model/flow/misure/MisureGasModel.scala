package it.eng.au.portaleConsumi.model.flow.misure

import java.sql.Timestamp

/***
 * Classe per standardizzare le misure in input dai flussi
 */
case class MisureGasModel(
                           codice_pdr: String = null,
                           lettura: Integer = null,
                           data_lettura: Timestamp = null,
                           motivazione: String = null,
                           data_caricamento: Timestamp = null,
                           annomese: String = null,
                           flusso: String = null
)
