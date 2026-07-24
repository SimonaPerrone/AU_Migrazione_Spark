package it.eng.au.aggregatoreConsumiCdp.model

case class CaPreFinal(
                       id_sag_ann: String = null,
                       n_id_distr: String = null,
                       n_id_az_udd: String = null,
                       n_id_udb: String = null,
                       codice_remi: String = null,
                       codice_pdr: String = null,
                       calcmode: String = null,
                       cat_uso: String = null,
                       classe_prelievo: String = null,
                       zona_climatica: String = null,
                       id_reg_clim: String = null,
                       cod_prof_prel_std: String = null,
                       prelievo_annuo_prev: String = null,
                       prelievo_annuo_prev_forced: String = null,
                       trattamento: String = null,
                       anno_competenza: String = null,
                       executionid: Long = 0
                     )
