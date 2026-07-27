CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.sw_con_tra_view_pod_riconf
(
pod_config string
, d_data_decorrenza string
, t_tipo_configurazione string
, d_creazione timestamp
)
PARTITIONED BY
(
annomese_sw string
)
STORED AS PARQUET;
