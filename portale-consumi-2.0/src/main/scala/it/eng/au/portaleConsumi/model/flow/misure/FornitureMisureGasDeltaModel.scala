package it.eng.au.portaleConsumi.model.flow.misure

import java.sql.Timestamp

/***
 * Classe contenente tutti i dettagli delle misure che devono essere caricate a DB
 */
case class FornitureMisureGasDeltaModel(
                                         codice_pdr: String = null,
                                         codice_fornitura: String = null,
                                         lettura: Integer = null,
                                         delta_misure: Integer = null,
                                         data_lettura: Timestamp = null,
                                         annomese: String = null,
                                         tipo_misura: String = null,
                                         categoria_misura: String = null
                                       )
