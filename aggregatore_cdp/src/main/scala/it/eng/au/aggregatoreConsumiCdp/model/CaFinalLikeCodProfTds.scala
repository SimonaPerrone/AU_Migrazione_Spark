package it.eng.au.aggregatoreConsumiCdp.model

import java.sql.Timestamp

case class CaFinalLikeCodProfTds(
                             codice_pdr: String = null,
                             n_id_distr: String = null,
                             n_id_udd: String = null,
                             n_id_udb: String = null,
                             cat_uso: String = null,
                             cod_remi: String = null,
                             zona_climatica: String = null,
                             classe_prelievo: String = null,
                             id_regione_climatica: String = null,
                             prelievo_annuo_prev: String = null,
                             trattamento: String = null,
                             cod_prof_prel_std: String = null,
                             cod_prof_prel_std_calc: String = null,
                             data_fine_for: Timestamp = null,
                             data_creazione: Timestamp = null,
                             error_log: String = null,
                             cod_prof_prel_std_forced: String = null,
                             prelievo_annuo_prev_forced: String = null,
                             cat_uso_forced: String = null,
                             zona_climatica_forced: String = null,
                             classe_prelievo_forced: String = null,
                             trattamento_forced: String = null,
                             pres_tds: String = null,
                             massivo_freeze_execution_id: Long = 0,
                             massivo_freeze_date: Timestamp = null,
                             anno_competenza: Int = 0,
                             execution_id: Long = 0
                           )