package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class VRcugasDistributorePModel(
                                      n_id_distributore: String = null,
                                      n_id_azienda: String = null,
                                      t_codice_esercente: String = null,
                                      n_id_utente: String = null,
                                      t_codice_aeeg: String = null,
                                      t_piva: String = null,
                                      t_rag_soc: String = null,
                                      d_data_inizio: Timestamp = null,
                                      d_data_fine: Timestamp = null
                                    )
