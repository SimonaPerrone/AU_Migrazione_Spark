package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasIndirizziPModel(
                                  n_id: String = null,
                                  t_toponimo: String = null,
                                  t_nomestrada: String = null,
                                  t_civico: String = null,
                                  t_comune: String = null,
                                  t_comune_istat: String = null,
                                  t_provincia: String = null,
                                  t_nazione: String = null,
                                  t_indirizzo_completo: String = null,
                                  t_presso: String = null,
                                  d_aggiornamento: Timestamp = null,
                                  n_id_traccia: String = null,
                                  n_id_s_prec: String = null,
                                  d_data_rif: Timestamp = null,
                                  t_cap: String = null
                                )
