package it.eng.au.portale_consumi_ee.model.rcu

case class RcuTariffaPModel(
                             n_id_tariffa: String = null,
                             n_id_fornitura: String = null,
                             t_tariffa_distr: String = null,
                             d_inizio_tariffa: String = null,
                             d_fine_tariffa: String = null,
                             b_storico: String = null,
                             b_valido: String = null,
                             b_ultima: String = null,
                             n_id_traccia: String = null,
                             d_aggiornamento: String = null
                      )
