CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.rcu_fasce_misuratore_2g
(
n_id_fasce_misuratore_2g string
, n_id_misuratore string
, n_cod_giorno_2g bigint
, d_data_giorno string
, n_fascia_1 bigint
, n_fine_fascia_1 bigint
, n_fascia_2 bigint
, n_fine_fascia_2 bigint
, n_fascia_3 bigint
, n_fine_fascia_3 bigint
, n_fascia_4 bigint
, n_fine_fascia_4 bigint
, n_fascia_5 bigint
, n_fine_fascia_5 bigint
, n_fascia_6 bigint
, n_fine_fascia_6 bigint
, n_fascia_7 bigint
, n_fine_fascia_7 bigint
, n_fascia_8 bigint
, n_fine_fascia_8 bigint
, n_fascia_9 bigint
, n_fine_fascia_9 bigint
, n_fascia_10 bigint
, n_fine_fascia_10 bigint
, t_nota string
, d_aggiornamento string
, n_id_traccia string
, n_id_s_prec string
, d_data_rif string
, d_creazione string
)
PARTITIONED BY
(
annomese_upd string
)
STORED AS PARQUET;
