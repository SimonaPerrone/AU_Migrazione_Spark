package it.eng.au.pubblicazione_cce.model.cce

///TODO: diverso schema tra calcolo spark e documentazione.. verificare
case class CceCalcTrackModel(
                              t_tipo_calc: String,
                              t_mode_calc: String,
                              t_anno_calc: String,
                              t_mese_calc: String,
                              d_data_calc: String,
                              t_esito: String,
                              executionid: String
                            )
