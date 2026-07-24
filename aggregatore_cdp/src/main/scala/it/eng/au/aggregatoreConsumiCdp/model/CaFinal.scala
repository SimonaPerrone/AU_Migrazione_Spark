package it.eng.au.aggregatoreConsumiCdp.model

case class CaFinal(
                    n_id_distr: String = null,
                    n_id_az_udd: String = null,
                    n_id_udb: String = null,
                    piva_distr: String = null,
                    piva_udd: String = null,
                    piva_udb: String = null,
                    codice_remi: String = null,
                    codice_pdr: String = null,
                    cat_uso: String = null,
                    classe_prelievo: String = null,
                    zona_climatica: String = null,
                    id_reg_clim: String = null,
                    cod_prof_prel_std: String = null,
                    prelievo_annuo_prev: String = null,
                    trattamento: String = null,
                    pres_tds: String = null,
                    anno_competenza: String = null,
                    massivo_freeze_executionid: Long = 0,
                    freeze_date: String = null,
                    tipo_trasmissione: String = null,
                    executionid: Long = 0
                  )
