package it.eng.au.pubblicazioneIndennizzi.model

case class AggregatoTotale(
                            id_indennizzo: Long = 0L,
                            piva_distr: String = "",
                            rag_soc_distr: String = "",
                            piva_udd: String = "",
                            rag_soc_udd: String = "",
                            pdr_g: Long = 0L,
                            pdr_g_om1: Long = 0L,
                            pdr_g_om2: Long = 0L,
                            pdr_g_om3: Long = 0L,
                            achieved_percentage_om1: Double = 0.0,
                            achieved_percentage_om2: Double = 0.0,
                            achieved_percentage_om3: Double = 0.0,
                            pdr_target_om1: Double = 0.0,
                            pdr_target_om2: Double = 0.0,
                            pdr_target_om3: Double = 0.0,
                            delta_pdr_om1: Double = 0.0,
                            delta_pdr_om2: Double = 0.0,
                            delta_pdr_om3: Double = 0.0,
                            indennizzo_om1: Double = 0.0,
                            indennizzo_om2: Double = 0.0,
                            indennizzo_om3: Double = 0.0,
                            annomese: String = "",
                            executionid: Long = 0L
                          )
