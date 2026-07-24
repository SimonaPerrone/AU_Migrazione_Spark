DROP TABLE IF EXISTS ${hive.table.cceCalcoloCa};
CREATE TABLE ${hive.table.cceCalcoloCa} (
    n_id_richiesta STRING,
    anno STRING,
    cod_pod STRING,
    piva_distr STRING,
    piva_udd STRING,
    ca DOUBLE,
    data_aggiornamento STRING,
    d_data_elaborazione STRING
)
STORED AS PARQUET;
