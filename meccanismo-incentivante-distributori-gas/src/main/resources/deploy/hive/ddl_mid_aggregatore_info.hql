DROP TABLE IF EXISTS ${hive.table.mid_aggregatore_info};
CREATE TABLE ${hive.table.mid_aggregatore_info} (
    operation_name STRING,
    nome_file STRING,
    path STRING,
    tipo_dest STRING,
    piva_dest STRING,
    piva_id_file STRING,
    piva_udd_file STRING,
    data_caricamento DATE,
    executionid_mid_dettaglio BIGINT
)
PARTITIONED BY (executionid BIGINT)
STORED AS PARQUET;