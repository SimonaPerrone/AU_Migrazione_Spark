package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasVenditorePModel(
                             n_id_venditore: String = null,
                             n_id_azienda: String = null,
                             t_codice_map: String = null,
                             d_data_inizio: Timestamp = null,
                             d_data_fine: Timestamp = null,
                             t_note: String = null,
                             d_aggiornamento: Timestamp = null,
                             n_id_traccia: String = null,
                             n_id_s_prec: String = null,
                             d_data_rif: Timestamp = null
                           )
