package it.eng.au.pubblicazione_cce.model.flow

// aggregazione di calc track in cui per ogni anno mese ho il max executionid valido
case class CalcTrackAggModel(
                              t_anno_calc: String,
                              t_mese_calc: String,
                              d_data_calc: String,
                              executionid: String
                            )
