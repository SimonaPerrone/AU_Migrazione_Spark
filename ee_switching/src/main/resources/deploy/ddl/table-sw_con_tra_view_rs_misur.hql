CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.sw_con_tra_view_rs_misur
(
d_data_apertura_m string
, t_cod_contr_disp string
, n_id_pratica double
, pod14 string
, d_data_decorrenza string
, n_id_utente_distr string
, t_protocollo string
, n_id_utente_udd string
, da_attivare_mese_succ double
, attivati_nel_mese double
, split_pod double
, t_codice_pod string
, d_anno_mese string
, trattamento_online string
, piva_distr string
, piva_udd string
, anno_mese_calc string
, tipo_misuratore string
, data_inst_mis string
, d_creazione timestamp
)
PARTITIONED BY
(
annomese_sw string
)
STORED AS PARQUET;
