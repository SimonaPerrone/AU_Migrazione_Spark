package it.eng.au.portale_consumi_ee.model.mongodbs

case class FornitureModel(
                           n_id_fornitura: String = null,
                           inizio:  java.lang.Long = null,
                           fine:  java.lang.Long = null,
                           d_inizio_str: String = null,
                           d_fine_str: String = null,
                           codice_pod: String = null,
                           attivo: String = null,
                           n_id_pod: String = null,
                           n_id_fornitore: String = null,
                           t_tipo_mercato: String = null,
                           n_id_cliente: String = null,
                           n_id_indirizzo: String = null,
                           n_id_ind_forn: String = null,
                           t_servizio_tutela_sii: String = null
                         )
