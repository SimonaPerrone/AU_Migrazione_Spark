package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasResidenzaPModel(
                                  n_id_residenza: String = null,
                                  n_id_fornitura: String = null,
                                  t_residenza: String = null,
                                  d_data_inizio: Timestamp = null,
                                  d_data_fine: Timestamp = null,
                                  d_aggiornamento: Timestamp = null,
                                  n_id_traccia: String = null,
                                  n_id_s_prec: String = null,
                                  d_data_rif: Timestamp = null
                                )
