package it.eng.au.portale_consumi_ee.model.mongodbs

case class FornitureInfoModel(
                               n_id_fornitura: String = null,
                               n_id_pod: String = null,
                               n_id_cliente: String = null,
                               d_inizio_titolarita :  java.lang.Long = null,
                               d_fine_titolarita:  java.lang.Long = null,
                               d_inizio_titolarita_str: String = null,
                               d_fine_titolarita_str: String = null,
                               n_id_fornitore: String = null,
                               t_tipo_mercato: String = null,
                               n_id_indirizzo: String = null,
                               n_id_ind_forn: String = null,
                               codice_pod: String = null,
                               t_residente: String = null,
                               t_tariffa_distr: String = null,
                               t_piva: String = null,
                               t_rag_soc: String = null,
                               t_servizio_tutela_sii: String = null
                         )
