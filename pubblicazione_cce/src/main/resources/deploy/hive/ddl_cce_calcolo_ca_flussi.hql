DROP TABLE IF EXISTS ${hive.table.cceCalcoloCaFlussi};
CREATE TABLE ${hive.table.cceCalcoloCaFlussi} (
    n_id_richiesta STRING,
    pod STRING,
    path_cloud STRING,
    data_aggiornamento STRING
)
STORED AS PARQUET;
