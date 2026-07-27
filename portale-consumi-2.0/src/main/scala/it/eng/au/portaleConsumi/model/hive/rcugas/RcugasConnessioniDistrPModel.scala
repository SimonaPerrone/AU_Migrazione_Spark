package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasConnessioniDistrPModel(
                                         t_codice_pdr: String = null,
                                         n_id_pdr: String = null,
                                         n_id_remi: String = null,
                                         d_data_inizio_conn: Timestamp = null,
                                         d_data_fine_conn: Timestamp = null,
                                         t_remi: String = null,
                                         n_id_distr: String = null,
                                         d_data_inizio_gestecn: Timestamp = null,
                                         d_data_fine_gestecn: Timestamp = null,
                                         t_remi_rcu: String = null,
                                         id_regione_climatica: String = null,
                                         t_piva_distr: String = null
                                       )
