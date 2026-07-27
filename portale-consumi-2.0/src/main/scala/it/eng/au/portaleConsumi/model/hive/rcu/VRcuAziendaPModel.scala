package it.eng.au.portaleConsumi.model.hive.rcu

import java.sql.Timestamp

case class VRcuAziendaPModel(
                              n_id_azienda: String = null,
                              n_id_utente: String = null,
                              t_codice_aeeg: String = null,
                              t_piva: String = null,
                              t_cf: String = null,
                              t_rag_soc: String = null,
                              n_id_sedelegale: String = null,
                              t_contatto: String = null,
                              t_email: String = null,
                              t_pec: String = null,
                              d_aggiornamento: Timestamp = null,
                              n_id_traccia: String = null,
                              n_id_s_prec: String = null,
                              t_ruoli: String = null,
                              t_desc_ruoli: String = null
                            )
