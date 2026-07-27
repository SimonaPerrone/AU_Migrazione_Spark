CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.rcus_podtecn
(
n_id_scheda string
, n_id_pod string
, n_potenza_disponibile double
, n_potenza_impegnata double
, n_tensione int
, t_tipo_misuratore string
, n_k_trasformazione double
, d_inst_misuratore string
, d_rimoz_misuratore string
, t_nota string
, d_aggiornamento string
, d_archiviazione string
, n_id_traccia string
, n_id_s_prec string
, n_id_s_succ string
, b_valido string
, n_num_cifre_ea int
, n_num_cifre_er int
, n_k_trasfor_att double
, n_k_trasfor_rea double
, n_k_trasfor_pot double
, t_mat_misuratore_att string
, t_mat_misuratore_rea string
, t_mat_misuratore_pot string
, d_inst_misurator_att string
, d_inst_misurator_rea string
, d_inst_misurator_pot string
, n_num_cifre_att int
, n_num_cifre_rea int
, n_num_cifre_pot int
, b_presenza_mis string
, b_gest_forfait string
, t_tipo_pod string
, d_fine_tipo_pod string
, d_oper_misurator_att string
, d_oper_misurator_rea string
, d_oper_misurator_pot string
, t_motivazione string
, d_creazione string
)
PARTITIONED BY
(
annomese_upd string
)
STORED AS PARQUET;
