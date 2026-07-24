package it.eng.au.pubblicazione_cce.model.cce

import java.sql.Timestamp

///TODO: diverso schema tra calcolo spark e documentazione.. verificare
case class CceCalcoloTrattamentoModel(
                          t_codice_pod: String,
                          n_id_pod: String,
                          t_anno_mese: String,
                          d_data_elaborazione: String,
                          is_t_trattamento: String
                        )
