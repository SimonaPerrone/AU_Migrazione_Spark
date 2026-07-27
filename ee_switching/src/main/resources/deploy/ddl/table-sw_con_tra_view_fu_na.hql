CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.sw_con_tra_view_fu_na
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
, pod string
, t_tipo_misuratore string
, t_area_rif string
, n_tensione double
, n_potenza_disponibile double
, n_potenza_impegnata double
, n_k_trasfor_att double
, n_k_trasfor_rea double
, n_k_trasfor_pot double
, t_mat_misuratore_att string
, t_mat_misuratore_rea string
, t_mat_misuratore_pot string
, d_inst_misurator_att string
, d_inst_misurator_rea string
, d_inst_misurator_pot string
, n_num_cifre_att double
, n_num_cifre_rea double
, n_num_cifre_pot double
, b_presenza_mis string
, b_gest_forfait string
, d_regime string
, pod_fornitura string
, t_diritto_tutela string
, b_disalimentabilita string
, piva_distr string
, piva_udd string
, t_residente string
, t_tariffa_distr string
, t_tipo_configurazione string
, d_creazione timestamp
)
PARTITIONED BY
(
annomese_sw string
)
STORED AS PARQUET;
