DROP TABLE IF EXISTS ${gse.db}.gse_aggr_a_export;
CREATE EXTERNAL TABLE ${gse.db}.gse_aggr_a_export(
    n_id_gse_richiesta_er_a BIGINT,
    t_anno STRING,
    t_mese STRING,
    t_cod_pod STRING,
    n_consumo_mensile DECIMAL(12, 3),
    d_data_creazione TIMESTAMP,
    n_execution_id BIGINT
)
STORED AS PARQUET
LOCATION '${hdfs.output.path}/gse_aggr_a_export';