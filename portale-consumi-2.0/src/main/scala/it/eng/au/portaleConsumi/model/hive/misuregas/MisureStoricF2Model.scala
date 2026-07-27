package it.eng.au.portaleConsumi.model.hive.misuregas

import java.sql.Timestamp

case class MisureStoricF2Model(
                                cf_piva: String = null,
                                pdr: String = null,
                                annomese_riferimento: String = null,
                                data_lettura: Timestamp = null,
                                dt_caricamento: Timestamp = null,
                                flusso: String = null,
                                motivazione: String = null,
                                let_tol_prel: String = null,
                                cod_pdr: String = null,
                                seed: String = null
                              )
