package it.eng.au.mid.model.hive.rcugas

import java.sql.Timestamp

case class RcugasConnessioniDistr2RemiPModel(
                                              t_codice_pdr: String = null,
                                              n_id_pdr: String = null,
                                              n_id_remi: String = null,
                                              d_data_inizio_conn: Timestamp = null,
                                              d_data_fine_conn: Timestamp = null,
                                              d_data_inizio_aggregazione: Timestamp = null,
                                              d_data_fine_aggregazione: Timestamp = null,
                                              t_piva_distr: String = null
                                            )
