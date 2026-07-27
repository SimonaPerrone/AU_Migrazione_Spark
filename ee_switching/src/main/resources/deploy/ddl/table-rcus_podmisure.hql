CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.rcus_podmisure
(
n_id_scheda string
, n_id_pod string
, d_anno_mese string
, t_trattamento string
, t_trattamento_succ string
, n_consumo_annuo double
, t_nota string
, d_aggiornamento string
, d_archiviazione string
, n_id_traccia string
, n_id_s_prec string
, n_id_s_succ string
, b_valido string
, d_creazione string
)
PARTITIONED BY
(
annomese_upd string
)
STORED AS PARQUET;
