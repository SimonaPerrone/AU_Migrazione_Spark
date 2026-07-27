DROP TABLE IF EXISTS ${hive.table.mid_contatori};
CREATE TABLE ${hive.table.mid_contatori} (
    pdr STRING,
    contatore INT,
    stato STRING,
    treatment STRING,
    data_tracciatura DATE,
    processo_tracciatura STRING,
    sessione_tracciatura STRING,
    causale_tracciatura STRING,
    tipo_calcolo STRING,
    executionid_daily_consumption BIGINT,
    executionid_tracciatura_prev BIGINT
)
PARTITIONED BY (annomese STRING, executionid_tracciatura BIGINT)
STORED AS PARQUET;