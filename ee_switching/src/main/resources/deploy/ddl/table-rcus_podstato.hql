CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.rcus_podstato
(
n_id_scheda string
, n_id_pod string
, t_stato_attivazione string
, d_attivazione string
, d_disattivazione string
, t_causale_no_riattiv string
, t_causale_no_disattiv string
, t_stato_sosp string
, d_sospensione string
, d_revoca_sosp string
, t_causale_no_sosp string
, t_switching string
, t_nota string
, d_aggiornamento string
, d_archiviazione string
, n_id_traccia string
, n_id_s_prec string
, n_id_s_succ string
, b_valido string
, t_cod_disattivazione string
, d_creazione string
)
PARTITIONED BY
(
annomese_upd string
)
STORED AS PARQUET;
