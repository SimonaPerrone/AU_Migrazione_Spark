CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.rcu_misuratore_2g
(
n_id_misuratore_2g string
, n_id_pod string
, b_vis_fasce string
, b_vis_venditore string
, b_vis_telefonov string
, b_vis_datainicontr string
, b_vis_datainiziofreezing string
, b_vis_messaggicliente string
, b_vis_codcli string
, t_codcli string
, t_venditore string
, t_telefonov string
, d_data_inicontr string
, d_data_iniziofreezing string
, t_messaggio_cliente_1 string
, t_messaggio_cliente_2 string
, t_messaggio_cliente_3 string
, t_messaggio_cliente_4 string
, t_messaggio_cliente_5 string
, n_num_fasce bigint
, d_inizio_validita string
, d_fine_validita string
, t_nota string
, d_aggiornamento string
, n_id_traccia string
, n_id_s_prec string
, d_data_rif string
, t_tipo_configurazione string
, d_creazione string
)
PARTITIONED BY
(
annomese_upd string
)
STORED AS PARQUET;
