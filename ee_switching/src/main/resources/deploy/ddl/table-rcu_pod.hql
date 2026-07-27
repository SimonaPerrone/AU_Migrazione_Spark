CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.rcu_pod
(
n_id_pod string
, t_codice_pod string
, t_area_rif string
, b_rich_indennizzo string
, b_rich_prest_distr string
, n_id_indirizzo string
, t_nota string
, d_aggiornamento string
, n_id_traccia string
, n_id_s_prec string
, n_id_ind_forn string
, d_creazione string
)
PARTITIONED BY
(
annomese_upd string
)
STORED AS PARQUET;
