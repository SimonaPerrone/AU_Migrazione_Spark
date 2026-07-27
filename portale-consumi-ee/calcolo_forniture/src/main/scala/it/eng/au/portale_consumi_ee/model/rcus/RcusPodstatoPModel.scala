package it.eng.au.portale_consumi_ee.model.rcus

case class RcusPodstatoPModel(
                               n_id_scheda: String = null,
                               n_id_pod: String = null,
                               t_stato_attivazione: String = null,
                               d_attivazione: String = null,
                               d_disattivazione: String = null,
                               t_causale_no_riattiv: String = null,
                               t_causale_no_disattiv: String = null,
                               t_stato_sosp: String = null,
                               d_sospensione: String = null,
                               d_revoca_sosp: String = null,
                               t_causale_no_sosp: String = null,
                               t_switching: String = null,
                               t_nota: String = null,
                               d_aggiornamento: String = null,
                               d_archiviazione: String = null,
                               n_id_traccia: String = null,
                               n_id_s_prec: String = null,
                               n_id_s_succ: String = null,
                               b_valido: String = null,
                               t_cod_disattivazione: String = null
                             )
