package it.eng.au.pubblicazione_cce.model.cce


case class CceCalcoloCaModel(
                              n_id_richiesta: String,
                              anno: String,
                              cod_pod: String,
                              piva_distr: String,
                              piva_udd: String,
                              ca: Double = 0.0,
                              data_aggiornamento: String,
                              d_data_elaborazione: String //todo-present??
                            )
