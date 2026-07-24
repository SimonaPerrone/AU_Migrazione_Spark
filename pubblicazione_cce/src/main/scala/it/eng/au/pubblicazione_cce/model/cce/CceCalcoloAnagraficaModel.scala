package it.eng.au.pubblicazione_cce.model.cce

case class CceCalcoloAnagraficaModel(
                                      t_codice_pod: String,
                                      n_id_pod: Long,
                                      t_area_rif: String,
                                      n_tensione: String,
                                      t_tensione: String,
                                      t_tipo_pod: String,
                                      t_piva_id: String,
                                      t_piva_udd: String,
                                      d_inizio_udd: String,
                                      d_fine_udd: String,
                                      t_codice_terna: String,
                                      t_tariffa_distr: String
                                    )
